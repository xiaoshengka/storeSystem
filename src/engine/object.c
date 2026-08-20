#include "engine/object.h"

#include "engine/hash.h"

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define ZSKIPLIST_MAXLEVEL 32
#define ZSKIPLIST_THRESHOLD ((UINT64_MAX / 4U))

typedef struct hash_value {
    unsigned char *data;
    size_t length;
} hash_value_t;

typedef struct zmember zmember_t;

typedef struct skip_node skip_node_t;
typedef struct skip_level {
    skip_node_t *forward;
    size_t span;
} skip_level_t;

struct skip_node {
    double score;
    const unsigned char *member;
    size_t member_length;
    skip_node_t *backward;
    int level_count;
    skip_level_t levels[];
};

typedef struct skiplist {
    skip_node_t *header;
    skip_node_t *tail;
    int level;
    size_t length;
    uint64_t random_state;
} skiplist_t;

enum rb_color {
    RB_RED,
    RB_BLACK
};

typedef struct rb_node {
    struct rb_node *left;
    struct rb_node *right;
    struct rb_node *parent;
    enum rb_color color;
    double score;
    const unsigned char *member;
    size_t member_length;
    size_t subtree_size;
} rb_node_t;

typedef struct rb_tree {
    rb_node_t *root;
    rb_node_t *nil;
    size_t length;
} rb_tree_t;

typedef struct zset {
    hashtable_t *members;
    kv_zset_engine_t engine;
    union {
        skiplist_t skiplist;
        rb_tree_t rbtree;
    } order;
} zset_t;

struct zmember {
    kv_hash_node_t *dict_node;
    void *order_node;
    double score;
};

struct kv_object {
    kv_object_type_t type;
    size_t memory_usage;
    union {
        struct {
            unsigned char *data;
            size_t length;
        } string;
        struct {
            hashtable_t *table;
        } hash;
        zset_t *zset;
    } value;
};

static int valid_bytes(const void *data, size_t length)
{
    return data != NULL || length == 0;
}

static unsigned char *copy_bytes(const void *data, size_t length)
{
    unsigned char *copy;

    if (!valid_bytes(data, length) || length == SIZE_MAX) {
        return NULL;
    }
    copy = malloc(length + 1U);
    if (copy == NULL) {
        return NULL;
    }
    if (length > 0) {
        memcpy(copy, data, length);
    }
    copy[length] = '\0';
    return copy;
}

static int compare_bytes(const void *left,
                         size_t left_length,
                         const void *right,
                         size_t right_length)
{
    size_t common = left_length < right_length ? left_length : right_length;
    int comparison = common == 0 ? 0 : memcmp(left, right, common);

    if (comparison != 0) return comparison;
    if (left_length < right_length) return -1;
    if (left_length > right_length) return 1;
    return 0;
}

static int compare_order(double left_score,
                         const void *left_member,
                         size_t left_length,
                         double right_score,
                         const void *right_member,
                         size_t right_length)
{
    if (left_score < right_score) return -1;
    if (left_score > right_score) return 1;
    return compare_bytes(left_member,
                         left_length,
                         right_member,
                         right_length);
}

static size_t skip_node_memory(int level)
{
    return sizeof(skip_node_t) + (size_t)level * sizeof(skip_level_t);
}

static uint64_t next_random(skiplist_t *list)
{
    uint64_t value = list->random_state;

    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    list->random_state = value;
    return value * UINT64_C(2685821657736338717);
}

static int skip_random_level(skiplist_t *list)
{
    int level = 1;

    while (level < ZSKIPLIST_MAXLEVEL &&
           next_random(list) <= ZSKIPLIST_THRESHOLD) {
        level++;
    }
    return level;
}

static skip_node_t *skip_node_create(int level, double score)
{
    skip_node_t *node = calloc(1, skip_node_memory(level));

    if (node != NULL) {
        node->score = score;
        node->level_count = level;
    }
    return node;
}

static int skiplist_init(skiplist_t *list)
{
    memset(list, 0, sizeof(*list));
    list->level = 1;
    list->random_state = UINT64_C(0x9e3779b97f4a7c15);
    list->header = skip_node_create(ZSKIPLIST_MAXLEVEL, 0.0);
    return list->header == NULL ? -1 : 0;
}

static void skiplist_destroy(skiplist_t *list)
{
    skip_node_t *node = list->header == NULL ? NULL
                                             : list->header->levels[0].forward;

    while (node != NULL) {
        skip_node_t *next = node->levels[0].forward;

        free(node);
        node = next;
    }
    free(list->header);
    memset(list, 0, sizeof(*list));
}

static void skiplist_insert_node(skiplist_t *list, skip_node_t *node)
{
    skip_node_t *update[ZSKIPLIST_MAXLEVEL];
    size_t rank[ZSKIPLIST_MAXLEVEL];
    skip_node_t *current = list->header;
    int index;

    for (index = list->level - 1; index >= 0; --index) {
        rank[index] = index == list->level - 1 ? 0 : rank[index + 1];
        while (current->levels[index].forward != NULL &&
               compare_order(current->levels[index].forward->score,
                             current->levels[index].forward->member,
                             current->levels[index].forward->member_length,
                             node->score,
                             node->member,
                             node->member_length) < 0) {
            rank[index] += current->levels[index].span;
            current = current->levels[index].forward;
        }
        update[index] = current;
    }
    if (node->level_count > list->level) {
        for (index = list->level; index < node->level_count; ++index) {
            rank[index] = 0;
            update[index] = list->header;
            update[index]->levels[index].span = list->length;
        }
        list->level = node->level_count;
    }
    for (index = 0; index < node->level_count; ++index) {
        node->levels[index].forward = update[index]->levels[index].forward;
        node->levels[index].span = update[index]->levels[index].span -
                                   (rank[0] - rank[index]);
        update[index]->levels[index].forward = node;
        update[index]->levels[index].span = rank[0] - rank[index] + 1U;
    }
    for (index = node->level_count; index < list->level; ++index) {
        update[index]->levels[index].span++;
    }
    node->backward = update[0] == list->header ? NULL : update[0];
    if (node->levels[0].forward != NULL) {
        node->levels[0].forward->backward = node;
    } else {
        list->tail = node;
    }
    list->length++;
}

static skip_node_t *skiplist_prepare_node(skiplist_t *list, double score)
{
    return skip_node_create(skip_random_level(list), score);
}

static void skiplist_remove_node(skiplist_t *list, skip_node_t *target)
{
    skip_node_t *update[ZSKIPLIST_MAXLEVEL];
    skip_node_t *current = list->header;
    int index;

    for (index = list->level - 1; index >= 0; --index) {
        while (current->levels[index].forward != NULL &&
               compare_order(current->levels[index].forward->score,
                             current->levels[index].forward->member,
                             current->levels[index].forward->member_length,
                             target->score,
                             target->member,
                             target->member_length) < 0) {
            current = current->levels[index].forward;
        }
        update[index] = current;
    }
    for (index = 0; index < list->level; ++index) {
        if (update[index]->levels[index].forward == target) {
            update[index]->levels[index].span += target->levels[index].span - 1U;
            update[index]->levels[index].forward = target->levels[index].forward;
        } else {
            update[index]->levels[index].span--;
        }
    }
    if (target->levels[0].forward != NULL) {
        target->levels[0].forward->backward = target->backward;
    } else {
        list->tail = target->backward;
    }
    while (list->level > 1 &&
           list->header->levels[list->level - 1].forward == NULL) {
        list->level--;
    }
    list->length--;
    free(target);
}

static skip_node_t *skiplist_by_rank(skiplist_t *list, size_t rank)
{
    skip_node_t *current = list->header;
    size_t traversed = 0;
    size_t wanted = rank + 1U;
    int index;

    for (index = list->level - 1; index >= 0; --index) {
        while (current->levels[index].forward != NULL &&
               traversed + current->levels[index].span <= wanted) {
            traversed += current->levels[index].span;
            current = current->levels[index].forward;
        }
        if (traversed == wanted) {
            return current;
        }
    }
    return NULL;
}

static void rb_recalculate(rb_node_t *node, rb_node_t *nil)
{
    if (node != nil) {
        node->subtree_size = node->left->subtree_size +
                             node->right->subtree_size + 1U;
    }
}

static void rb_recalculate_up(rb_tree_t *tree, rb_node_t *node)
{
    while (node != tree->nil) {
        rb_recalculate(node, tree->nil);
        node = node->parent;
    }
}

static int rbtree_init(rb_tree_t *tree)
{
    memset(tree, 0, sizeof(*tree));
    tree->nil = calloc(1, sizeof(*tree->nil));
    if (tree->nil == NULL) {
        return -1;
    }
    tree->nil->color = RB_BLACK;
    tree->nil->left = tree->nil;
    tree->nil->right = tree->nil;
    tree->nil->parent = tree->nil;
    tree->root = tree->nil;
    return 0;
}

static void rb_left_rotate(rb_tree_t *tree, rb_node_t *node)
{
    rb_node_t *right = node->right;

    node->right = right->left;
    if (right->left != tree->nil) right->left->parent = node;
    right->parent = node->parent;
    if (node->parent == tree->nil) tree->root = right;
    else if (node == node->parent->left) node->parent->left = right;
    else node->parent->right = right;
    right->left = node;
    node->parent = right;
    rb_recalculate(node, tree->nil);
    rb_recalculate(right, tree->nil);
}

static void rb_right_rotate(rb_tree_t *tree, rb_node_t *node)
{
    rb_node_t *left = node->left;

    node->left = left->right;
    if (left->right != tree->nil) left->right->parent = node;
    left->parent = node->parent;
    if (node->parent == tree->nil) tree->root = left;
    else if (node == node->parent->right) node->parent->right = left;
    else node->parent->left = left;
    left->right = node;
    node->parent = left;
    rb_recalculate(node, tree->nil);
    rb_recalculate(left, tree->nil);
}

static rb_node_t *rbtree_prepare_node(rb_tree_t *tree, double score)
{
    rb_node_t *node = calloc(1, sizeof(*node));

    if (node != NULL) {
        node->left = tree->nil;
        node->right = tree->nil;
        node->parent = tree->nil;
        node->color = RB_RED;
        node->score = score;
        node->subtree_size = 1U;
    }
    return node;
}

static void rbtree_insert_fixup(rb_tree_t *tree, rb_node_t *node)
{
    while (node->parent->color == RB_RED) {
        if (node->parent == node->parent->parent->left) {
            rb_node_t *uncle = node->parent->parent->right;

            if (uncle->color == RB_RED) {
                node->parent->color = RB_BLACK;
                uncle->color = RB_BLACK;
                node->parent->parent->color = RB_RED;
                node = node->parent->parent;
            } else {
                if (node == node->parent->right) {
                    node = node->parent;
                    rb_left_rotate(tree, node);
                }
                node->parent->color = RB_BLACK;
                node->parent->parent->color = RB_RED;
                rb_right_rotate(tree, node->parent->parent);
            }
        } else {
            rb_node_t *uncle = node->parent->parent->left;

            if (uncle->color == RB_RED) {
                node->parent->color = RB_BLACK;
                uncle->color = RB_BLACK;
                node->parent->parent->color = RB_RED;
                node = node->parent->parent;
            } else {
                if (node == node->parent->left) {
                    node = node->parent;
                    rb_right_rotate(tree, node);
                }
                node->parent->color = RB_BLACK;
                node->parent->parent->color = RB_RED;
                rb_left_rotate(tree, node->parent->parent);
            }
        }
    }
    tree->root->color = RB_BLACK;
}

static void rbtree_insert_node(rb_tree_t *tree, rb_node_t *node)
{
    rb_node_t *parent = tree->nil;
    rb_node_t *current = tree->root;

    while (current != tree->nil) {
        parent = current;
        current->subtree_size++;
        current = compare_order(node->score,
                                node->member,
                                node->member_length,
                                current->score,
                                current->member,
                                current->member_length) < 0
                      ? current->left
                      : current->right;
    }
    node->parent = parent;
    if (parent == tree->nil) tree->root = node;
    else if (compare_order(node->score,
                           node->member,
                           node->member_length,
                           parent->score,
                           parent->member,
                           parent->member_length) < 0) parent->left = node;
    else parent->right = node;
    rbtree_insert_fixup(tree, node);
    tree->length++;
}

static rb_node_t *rb_minimum(rb_tree_t *tree, rb_node_t *node)
{
    while (node->left != tree->nil) node = node->left;
    return node;
}

static rb_node_t *rb_successor(rb_tree_t *tree, rb_node_t *node)
{
    rb_node_t *parent;

    if (node->right != tree->nil) return rb_minimum(tree, node->right);
    parent = node->parent;
    while (parent != tree->nil && node == parent->right) {
        node = parent;
        parent = parent->parent;
    }
    return parent;
}

static void rb_transplant(rb_tree_t *tree, rb_node_t *old, rb_node_t *replacement)
{
    if (old->parent == tree->nil) tree->root = replacement;
    else if (old == old->parent->left) old->parent->left = replacement;
    else old->parent->right = replacement;
    replacement->parent = old->parent;
}

static void rbtree_delete_fixup(rb_tree_t *tree, rb_node_t *node)
{
    while (node != tree->root && node->color == RB_BLACK) {
        if (node == node->parent->left) {
            rb_node_t *sibling = node->parent->right;

            if (sibling->color == RB_RED) {
                sibling->color = RB_BLACK;
                node->parent->color = RB_RED;
                rb_left_rotate(tree, node->parent);
                sibling = node->parent->right;
            }
            if (sibling->left->color == RB_BLACK &&
                sibling->right->color == RB_BLACK) {
                sibling->color = RB_RED;
                node = node->parent;
            } else {
                if (sibling->right->color == RB_BLACK) {
                    sibling->left->color = RB_BLACK;
                    sibling->color = RB_RED;
                    rb_right_rotate(tree, sibling);
                    sibling = node->parent->right;
                }
                sibling->color = node->parent->color;
                node->parent->color = RB_BLACK;
                sibling->right->color = RB_BLACK;
                rb_left_rotate(tree, node->parent);
                node = tree->root;
            }
        } else {
            rb_node_t *sibling = node->parent->left;

            if (sibling->color == RB_RED) {
                sibling->color = RB_BLACK;
                node->parent->color = RB_RED;
                rb_right_rotate(tree, node->parent);
                sibling = node->parent->left;
            }
            if (sibling->right->color == RB_BLACK &&
                sibling->left->color == RB_BLACK) {
                sibling->color = RB_RED;
                node = node->parent;
            } else {
                if (sibling->left->color == RB_BLACK) {
                    sibling->right->color = RB_BLACK;
                    sibling->color = RB_RED;
                    rb_left_rotate(tree, sibling);
                    sibling = node->parent->left;
                }
                sibling->color = node->parent->color;
                node->parent->color = RB_BLACK;
                sibling->left->color = RB_BLACK;
                rb_right_rotate(tree, node->parent);
                node = tree->root;
            }
        }
    }
    node->color = RB_BLACK;
}

static void rbtree_remove_node(rb_tree_t *tree, rb_node_t *target)
{
    rb_node_t *moved = target;
    rb_node_t *fixup;
    enum rb_color original = moved->color;
    rb_node_t *recalculate_from;

    if (target->left == tree->nil) {
        fixup = target->right;
        recalculate_from = target->parent;
        rb_transplant(tree, target, target->right);
    } else if (target->right == tree->nil) {
        fixup = target->left;
        recalculate_from = target->parent;
        rb_transplant(tree, target, target->left);
    } else {
        moved = rb_minimum(tree, target->right);
        original = moved->color;
        fixup = moved->right;
        if (moved->parent == target) {
            fixup->parent = moved;
            recalculate_from = moved;
        } else {
            rb_node_t *old_parent = moved->parent;

            rb_transplant(tree, moved, moved->right);
            moved->right = target->right;
            moved->right->parent = moved;
            /* Recalculate only after moved replaces target.  Before that,
             * old_parent reaches moved through the adopted right subtree
             * while moved still points back into that subtree, forming a
             * temporary parent cycle. */
            recalculate_from = old_parent;
        }
        rb_transplant(tree, target, moved);
        moved->left = target->left;
        moved->left->parent = moved;
        moved->color = target->color;
        rb_recalculate(moved, tree->nil);
    }
    rb_recalculate_up(tree, recalculate_from);
    if (original == RB_BLACK) rbtree_delete_fixup(tree, fixup);
    rb_recalculate_up(tree, fixup->parent);
    tree->length--;
    free(target);
}

static rb_node_t *rbtree_by_rank(rb_tree_t *tree, size_t rank)
{
    rb_node_t *node = tree->root;

    while (node != tree->nil) {
        size_t left_size = node->left->subtree_size;

        if (rank < left_size) node = node->left;
        else if (rank == left_size) return node;
        else {
            rank -= left_size + 1U;
            node = node->right;
        }
    }
    return NULL;
}

static void rbtree_destroy_nodes(rb_tree_t *tree, rb_node_t *node)
{
    if (node != tree->nil) {
        rbtree_destroy_nodes(tree, node->left);
        rbtree_destroy_nodes(tree, node->right);
        free(node);
    }
}

static void rbtree_destroy(rb_tree_t *tree)
{
    if (tree->nil != NULL) {
        rbtree_destroy_nodes(tree, tree->root);
        free(tree->nil);
    }
    memset(tree, 0, sizeof(*tree));
}

static size_t ordered_base_memory(const zset_t *set)
{
    return set->engine == KV_ZSET_SKIPLIST
               ? skip_node_memory(ZSKIPLIST_MAXLEVEL)
               : sizeof(rb_node_t);
}

static void *ordered_prepare_node(zset_t *set, double score)
{
    return set->engine == KV_ZSET_SKIPLIST
               ? (void *)skiplist_prepare_node(&set->order.skiplist, score)
               : (void *)rbtree_prepare_node(&set->order.rbtree, score);
}

static size_t ordered_node_memory(const zset_t *set, const void *node)
{
    return set->engine == KV_ZSET_SKIPLIST
               ? skip_node_memory(((const skip_node_t *)node)->level_count)
               : sizeof(rb_node_t);
}

static void ordered_set_member(zset_t *set,
                               void *node,
                               const void *member,
                               size_t member_length)
{
    if (set->engine == KV_ZSET_SKIPLIST) {
        skip_node_t *skip_node = node;

        skip_node->member = member;
        skip_node->member_length = member_length;
    } else {
        rb_node_t *rb_node = node;

        rb_node->member = member;
        rb_node->member_length = member_length;
    }
}

static void ordered_insert_node(zset_t *set, void *node)
{
    if (set->engine == KV_ZSET_SKIPLIST) {
        skiplist_insert_node(&set->order.skiplist, node);
    } else {
        rbtree_insert_node(&set->order.rbtree, node);
    }
}

static void ordered_remove_node(zset_t *set, void *node)
{
    if (set->engine == KV_ZSET_SKIPLIST) {
        skiplist_remove_node(&set->order.skiplist, node);
    } else {
        rbtree_remove_node(&set->order.rbtree, node);
    }
}

static void ordered_free_prepared(void *node)
{
    free(node);
}

static void hash_value_destroy(void *payload)
{
    hash_value_t *value = payload;

    if (value != NULL) {
        free(value->data);
        free(value);
    }
}

static void zmember_destroy(void *payload)
{
    free(payload);
}

int kv_object_create_string(kv_object_t **out_object,
                            const void *value,
                            size_t value_length)
{
    kv_object_t *object;

    if (out_object == NULL || !valid_bytes(value, value_length)) return -1;
    *out_object = NULL;
    object = calloc(1, sizeof(*object));
    if (object == NULL) return -1;
    object->value.string.data = copy_bytes(value, value_length);
    if (object->value.string.data == NULL) {
        free(object);
        return -1;
    }
    object->type = KV_OBJECT_STRING;
    object->value.string.length = value_length;
    object->memory_usage = sizeof(*object) + value_length + 1U;
    *out_object = object;
    return 0;
}

int kv_object_create_hash(kv_object_t **out_object)
{
    kv_object_t *object;

    if (out_object == NULL) return -1;
    *out_object = NULL;
    object = calloc(1, sizeof(*object));
    if (object == NULL || kv_hash_create(&object->value.hash.table) != 0) {
        free(object);
        return -1;
    }
    object->type = KV_OBJECT_HASH;
    object->memory_usage = sizeof(*object) +
                           kv_hash_index_memory(object->value.hash.table);
    *out_object = object;
    return 0;
}

int kv_object_create_zset(kv_object_t **out_object, kv_zset_engine_t engine)
{
    kv_object_t *object;
    zset_t *set;

    if (out_object == NULL ||
        (engine != KV_ZSET_SKIPLIST && engine != KV_ZSET_RBTREE)) return -1;
    *out_object = NULL;
    object = calloc(1, sizeof(*object));
    set = calloc(1, sizeof(*set));
    if (object == NULL || set == NULL || kv_hash_create(&set->members) != 0) {
        free(set);
        free(object);
        return -1;
    }
    set->engine = engine;
    if ((engine == KV_ZSET_SKIPLIST &&
         skiplist_init(&set->order.skiplist) != 0) ||
        (engine == KV_ZSET_RBTREE && rbtree_init(&set->order.rbtree) != 0)) {
        kv_hash_release(set->members, NULL);
        free(set);
        free(object);
        return -1;
    }
    object->type = KV_OBJECT_ZSET;
    object->value.zset = set;
    object->memory_usage = sizeof(*object) + sizeof(*set) +
                           kv_hash_index_memory(set->members) +
                           ordered_base_memory(set);
    *out_object = object;
    return 0;
}

void kv_object_destroy(kv_object_t *object)
{
    if (object == NULL) return;
    if (object->type == KV_OBJECT_STRING) {
        free(object->value.string.data);
    } else if (object->type == KV_OBJECT_HASH) {
        kv_hash_release(object->value.hash.table, hash_value_destroy);
    } else if (object->type == KV_OBJECT_ZSET) {
        zset_t *set = object->value.zset;

        if (set->engine == KV_ZSET_SKIPLIST) skiplist_destroy(&set->order.skiplist);
        else rbtree_destroy(&set->order.rbtree);
        kv_hash_release(set->members, zmember_destroy);
        free(set);
    }
    free(object);
}

kv_object_type_t kv_object_type(const kv_object_t *object)
{
    return object == NULL ? KV_OBJECT_STRING : object->type;
}

size_t kv_object_memory_usage(const kv_object_t *object)
{
    return object == NULL ? 0 : object->memory_usage;
}

const void *kv_object_string_value(const kv_object_t *object,
                                   size_t *value_length)
{
    if (value_length != NULL) *value_length = 0;
    if (object == NULL || object->type != KV_OBJECT_STRING ||
        value_length == NULL) return NULL;
    *value_length = object->value.string.length;
    return object->value.string.data;
}

int kv_object_hash_set(kv_object_t *object,
                       const void *field,
                       size_t field_length,
                       const void *value,
                       size_t value_length,
                       int *added,
                       int *changed)
{
    kv_hash_node_t *node;
    hash_value_t *replacement;

    if (added != NULL) *added = 0;
    if (changed != NULL) *changed = 0;
    if (object == NULL || object->type != KV_OBJECT_HASH ||
        !valid_bytes(field, field_length) || !valid_bytes(value, value_length)) {
        return -1;
    }
    kv_hash_rehash_step(object->value.hash.table, 1U);
    node = kv_hash_find(object->value.hash.table, field, field_length);
    if (node != NULL) {
        hash_value_t *old = kv_hash_node_payload(node);

        if (old->length == value_length &&
            (value_length == 0 || memcmp(old->data, value, value_length) == 0)) {
            return 0;
        }
        replacement = calloc(1, sizeof(*replacement));
        if (replacement == NULL) return -1;
        replacement->data = copy_bytes(value, value_length);
        if (replacement->data == NULL) {
            free(replacement);
            return -1;
        }
        replacement->length = value_length;
        object->memory_usage = object->memory_usage - old->length - 1U +
                               value_length + 1U;
        kv_hash_node_set_payload(node, replacement);
        hash_value_destroy(old);
        if (changed != NULL) *changed = 1;
        return 0;
    }
    replacement = calloc(1, sizeof(*replacement));
    if (replacement == NULL) return -1;
    replacement->data = copy_bytes(value, value_length);
    if (replacement->data == NULL) {
        free(replacement);
        return -1;
    }
    replacement->length = value_length;
    {
        size_t old_index = kv_hash_index_memory(object->value.hash.table);

        if (kv_hash_insert(object->value.hash.table,
                           field,
                           field_length,
                           replacement,
                           NULL) != 0) {
            hash_value_destroy(replacement);
            return -1;
        }
        object->memory_usage += kv_hash_index_memory(object->value.hash.table) -
                                old_index +
                                kv_hash_node_memory_for_key(field_length) +
                                sizeof(*replacement) + value_length + 1U;
    }
    if (added != NULL) *added = 1;
    if (changed != NULL) *changed = 1;
    return 0;
}

const void *kv_object_hash_get(kv_object_t *object,
                               const void *field,
                               size_t field_length,
                               size_t *value_length)
{
    kv_hash_node_t *node;
    hash_value_t *value;

    if (value_length != NULL) *value_length = 0;
    if (object == NULL || object->type != KV_OBJECT_HASH ||
        value_length == NULL || !valid_bytes(field, field_length)) return NULL;
    kv_hash_rehash_step(object->value.hash.table, 1U);
    node = kv_hash_find(object->value.hash.table, field, field_length);
    value = node == NULL ? NULL : kv_hash_node_payload(node);
    if (value == NULL) return NULL;
    *value_length = value->length;
    return value->data;
}

int kv_object_hash_delete(kv_object_t *object,
                          const void *field,
                          size_t field_length)
{
    kv_hash_node_t *node;
    hash_value_t *value;
    size_t charge;

    if (object == NULL || object->type != KV_OBJECT_HASH ||
        !valid_bytes(field, field_length)) return -1;
    node = kv_hash_find(object->value.hash.table, field, field_length);
    if (node == NULL) return 0;
    value = kv_hash_node_payload(node);
    charge = kv_hash_node_memory_for_key(field_length) + sizeof(*value) +
             value->length + 1U;
    value = kv_hash_remove_node(object->value.hash.table, node);
    hash_value_destroy(value);
    object->memory_usage -= charge;
    return 1;
}

size_t kv_object_hash_length(const kv_object_t *object)
{
    return object != NULL && object->type == KV_OBJECT_HASH
               ? kv_hash_count(object->value.hash.table)
               : 0;
}

int kv_object_hash_visit(kv_object_t *object,
                         kv_hash_visit_fn callback,
                         void *context)
{
    kv_hash_iterator_t iterator;
    kv_hash_node_t *node;

    if (object == NULL || object->type != KV_OBJECT_HASH || callback == NULL) {
        return -1;
    }
    kv_hash_iterator_begin(object->value.hash.table, &iterator);
    while ((node = kv_hash_iterator_next(&iterator)) != NULL) {
        hash_value_t *value = kv_hash_node_payload(node);

        if (callback(kv_hash_node_key(node),
                     kv_hash_node_key_length(node),
                     value->data,
                     value->length,
                     context) != 0) return -1;
    }
    return 0;
}

int kv_object_zset_add(kv_object_t *object,
                       double score,
                       const void *member,
                       size_t member_length,
                       int *added,
                       int *changed)
{
    zset_t *set;
    kv_hash_node_t *dict_node;
    zmember_t *entry;
    void *prepared;

    if (added != NULL) *added = 0;
    if (changed != NULL) *changed = 0;
    if (object == NULL || object->type != KV_OBJECT_ZSET || isnan(score) ||
        !valid_bytes(member, member_length)) return -1;
    set = object->value.zset;
    kv_hash_rehash_step(set->members, 1U);
    dict_node = kv_hash_find(set->members, member, member_length);
    if (dict_node != NULL) {
        entry = kv_hash_node_payload(dict_node);
        if (entry->score == score) return 0;
        prepared = ordered_prepare_node(set, score);
        if (prepared == NULL) return -1;
        ordered_set_member(set,
                           prepared,
                           kv_hash_node_key(dict_node),
                           kv_hash_node_key_length(dict_node));
        object->memory_usage -= ordered_node_memory(set, entry->order_node);
        ordered_remove_node(set, entry->order_node);
        entry->score = score;
        entry->order_node = prepared;
        ordered_insert_node(set, prepared);
        object->memory_usage += ordered_node_memory(set, prepared);
        if (changed != NULL) *changed = 1;
        return 0;
    }
    entry = calloc(1, sizeof(*entry));
    prepared = ordered_prepare_node(set, score);
    if (entry == NULL || prepared == NULL) {
        free(entry);
        ordered_free_prepared(prepared);
        return -1;
    }
    {
        size_t old_index = kv_hash_index_memory(set->members);

        if (kv_hash_insert(set->members,
                           member,
                           member_length,
                           entry,
                           &dict_node) != 0) {
            free(entry);
            ordered_free_prepared(prepared);
            return -1;
        }
        entry->dict_node = dict_node;
        entry->order_node = prepared;
        entry->score = score;
        ordered_set_member(set,
                           prepared,
                           kv_hash_node_key(dict_node),
                           kv_hash_node_key_length(dict_node));
        ordered_insert_node(set, prepared);
        object->memory_usage += kv_hash_index_memory(set->members) - old_index +
                                kv_hash_node_memory_for_key(member_length) +
                                sizeof(*entry) +
                                ordered_node_memory(set, prepared);
    }
    if (added != NULL) *added = 1;
    if (changed != NULL) *changed = 1;
    return 0;
}

int kv_object_zset_score(kv_object_t *object,
                         const void *member,
                         size_t member_length,
                         double *score)
{
    kv_hash_node_t *node;
    zmember_t *entry;

    if (object == NULL || object->type != KV_OBJECT_ZSET || score == NULL ||
        !valid_bytes(member, member_length)) return -1;
    kv_hash_rehash_step(object->value.zset->members, 1U);
    node = kv_hash_find(object->value.zset->members, member, member_length);
    if (node == NULL) return 0;
    entry = kv_hash_node_payload(node);
    *score = entry->score;
    return 1;
}

int kv_object_zset_remove(kv_object_t *object,
                          const void *member,
                          size_t member_length)
{
    zset_t *set;
    kv_hash_node_t *node;
    zmember_t *entry;
    size_t charge;

    if (object == NULL || object->type != KV_OBJECT_ZSET ||
        !valid_bytes(member, member_length)) return -1;
    set = object->value.zset;
    node = kv_hash_find(set->members, member, member_length);
    if (node == NULL) return 0;
    entry = kv_hash_node_payload(node);
    charge = kv_hash_node_memory_for_key(member_length) + sizeof(*entry) +
             ordered_node_memory(set, entry->order_node);
    ordered_remove_node(set, entry->order_node);
    entry = kv_hash_remove_node(set->members, node);
    free(entry);
    object->memory_usage -= charge;
    return 1;
}

size_t kv_object_zset_length(const kv_object_t *object)
{
    return object != NULL && object->type == KV_OBJECT_ZSET
               ? kv_hash_count(object->value.zset->members)
               : 0;
}

int kv_object_zset_range(kv_object_t *object,
                         long long start,
                         long long stop,
                         kv_zset_visit_fn callback,
                         void *context,
                         size_t *visited)
{
    zset_t *set;
    size_t length;
    size_t first;
    size_t last;
    size_t count = 0;

    if (visited != NULL) *visited = 0;
    if (object == NULL || object->type != KV_OBJECT_ZSET || callback == NULL ||
        visited == NULL) return -1;
    set = object->value.zset;
    length = kv_hash_count(set->members);
    if (length == 0) return 0;
    if (start < 0) start += (long long)length;
    if (stop < 0) stop += (long long)length;
    if (start < 0) start = 0;
    if (stop < 0 || start >= (long long)length || start > stop) return 0;
    if (stop >= (long long)length) stop = (long long)length - 1;
    first = (size_t)start;
    last = (size_t)stop;
    if (set->engine == KV_ZSET_SKIPLIST) {
        skip_node_t *node = skiplist_by_rank(&set->order.skiplist, first);

        while (node != NULL && first + count <= last) {
            if (callback(node->member,
                         node->member_length,
                         node->score,
                         context) != 0) return -1;
            count++;
            node = node->levels[0].forward;
        }
    } else {
        rb_node_t *node = rbtree_by_rank(&set->order.rbtree, first);

        while (node != NULL && node != set->order.rbtree.nil &&
               first + count <= last) {
            if (callback(node->member,
                         node->member_length,
                         node->score,
                         context) != 0) return -1;
            count++;
            node = rb_successor(&set->order.rbtree, node);
        }
    }
    *visited = count;
    return 0;
}

kv_zset_engine_t kv_object_zset_engine(const kv_object_t *object)
{
    return object != NULL && object->type == KV_OBJECT_ZSET
               ? object->value.zset->engine
               : KV_ZSET_SKIPLIST;
}

const char *kv_zset_engine_name(kv_zset_engine_t engine)
{
    return engine == KV_ZSET_RBTREE ? "rbtree" : "skiplist";
}

typedef struct rb_validation {
    const rb_node_t *previous;
    size_t count;
    int valid;
} rb_validation_t;

static size_t validate_rb_node(const rb_tree_t *tree,
                               const rb_node_t *node,
                               const rb_node_t *parent,
                               rb_validation_t *validation,
                               size_t *black_height)
{
    size_t left_count;
    size_t right_count;
    size_t left_black_height;
    size_t right_black_height;

    if (node == tree->nil) {
        *black_height = 1U;
        return 0;
    }
    if (node->parent != parent) validation->valid = 0;
    left_count = validate_rb_node(tree,
                                  node->left,
                                  node,
                                  validation,
                                  &left_black_height);
    if (validation->previous != NULL &&
        compare_order(validation->previous->score,
                      validation->previous->member,
                      validation->previous->member_length,
                      node->score,
                      node->member,
                      node->member_length) >= 0) validation->valid = 0;
    validation->previous = node;
    validation->count++;
    right_count = validate_rb_node(tree,
                                   node->right,
                                   node,
                                   validation,
                                   &right_black_height);
    if (node->color == RB_RED &&
        (node->left->color != RB_BLACK || node->right->color != RB_BLACK)) {
        validation->valid = 0;
    }
    if (left_black_height != right_black_height ||
        node->subtree_size != left_count + right_count + 1U) {
        validation->valid = 0;
    }
    *black_height = left_black_height + (node->color == RB_BLACK ? 1U : 0U);
    return left_count + right_count + 1U;
}

static int validate_skiplist(const skiplist_t *list)
{
    const skip_node_t *node = list->header->levels[0].forward;
    const skip_node_t *previous = NULL;
    size_t count = 0;

    while (node != NULL) {
        if (node->backward != previous ||
            (previous != NULL &&
             compare_order(previous->score,
                           previous->member,
                           previous->member_length,
                           node->score,
                           node->member,
                           node->member_length) >= 0)) return 0;
        if (skiplist_by_rank((skiplist_t *)list, count) != node) return 0;
        previous = node;
        node = node->levels[0].forward;
        count++;
    }
    return count == list->length && previous == list->tail;
}

int kv_object_zset_validate(const kv_object_t *object)
{
    const zset_t *set;

    if (object == NULL || object->type != KV_OBJECT_ZSET) return -1;
    set = object->value.zset;
    if (set->engine == KV_ZSET_SKIPLIST) {
        return validate_skiplist(&set->order.skiplist) &&
               set->order.skiplist.length == kv_hash_count(set->members);
    } else {
        const rb_tree_t *tree = &set->order.rbtree;
        rb_validation_t validation = {NULL, 0U, 1};
        size_t black_height;
        size_t count;

        if (tree->nil == NULL || tree->nil->color != RB_BLACK ||
            (tree->root != tree->nil &&
             (tree->root->color != RB_BLACK || tree->root->parent != tree->nil))) {
            return 0;
        }
        count = validate_rb_node(tree,
                                 tree->root,
                                 tree->nil,
                                 &validation,
                                 &black_height);
        (void)black_height;
        return validation.valid && validation.count == count &&
               count == tree->length && count == kv_hash_count(set->members);
    }
}

typedef struct clone_hash_context {
    kv_object_t *destination;
    int failed;
} clone_hash_context_t;

static int clone_hash_field(const void *field,
                            size_t field_length,
                            const void *value,
                            size_t value_length,
                            void *context)
{
    clone_hash_context_t *clone = context;

    if (kv_object_hash_set(clone->destination,
                           field,
                           field_length,
                           value,
                           value_length,
                           NULL,
                           NULL) != 0) {
        clone->failed = 1;
        return -1;
    }
    return 0;
}

typedef struct clone_zset_context {
    kv_object_t *destination;
    int failed;
} clone_zset_context_t;

static int clone_zset_member(const void *member,
                             size_t member_length,
                             double score,
                             void *context)
{
    clone_zset_context_t *clone = context;

    if (kv_object_zset_add(clone->destination,
                           score,
                           member,
                           member_length,
                           NULL,
                           NULL) != 0) {
        clone->failed = 1;
        return -1;
    }
    return 0;
}

int kv_object_clone(const kv_object_t *object, kv_object_t **out_object)
{
    kv_object_t *copy = NULL;

    if (object == NULL || out_object == NULL) return -1;
    *out_object = NULL;
    if (object->type == KV_OBJECT_STRING) {
        return kv_object_create_string(out_object,
                                       object->value.string.data,
                                       object->value.string.length);
    }
    if (object->type == KV_OBJECT_HASH) {
        clone_hash_context_t context;

        if (kv_object_create_hash(&copy) != 0) return -1;
        context.destination = copy;
        context.failed = 0;
        if (kv_object_hash_visit((kv_object_t *)object,
                                 clone_hash_field,
                                 &context) != 0 || context.failed) {
            kv_object_destroy(copy);
            return -1;
        }
    } else {
        clone_zset_context_t context;
        size_t visited;

        if (kv_object_create_zset(&copy,
                                  object->value.zset->engine) != 0) return -1;
        context.destination = copy;
        context.failed = 0;
        if (kv_object_zset_range((kv_object_t *)object,
                                 0,
                                 -1,
                                 clone_zset_member,
                                 &context,
                                 &visited) != 0 || context.failed) {
            kv_object_destroy(copy);
            return -1;
        }
    }
    *out_object = copy;
    return 0;
}
