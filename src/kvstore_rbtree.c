


#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "kvstore.h"

#define RED				1
#define BLACK 			2


#define ENABLE_KEY_CHAR		1

#define MAX_KEY_LEN			256
#define MAX_VALUE_LEN		1024

#if ENABLE_KEY_CHAR
typedef char* KEY_TYPE;
#else
typedef int KEY_TYPE;
#endif


typedef struct _rbtree_node {
	unsigned char color;
	struct _rbtree_node *right;
	struct _rbtree_node *left;
	struct _rbtree_node *parent;

	KEY_TYPE key;
	void *value;
	size_t key_length;
	size_t value_length;
} rbtree_node;

typedef struct _rbtree {
	rbtree_node *root;
	rbtree_node *nil;
	
	int count;
} rbtree;

static int valid_bytes(const void *data, size_t length) {
	return data != NULL || length == 0;
}

static int compare_bytes(const void *left, size_t left_length,
						 const void *right, size_t right_length) {
	size_t common = left_length < right_length ? left_length : right_length;
	int result = common > 0 ? memcmp(left, right, common) : 0;

	if (result != 0) return result;
	if (left_length < right_length) return -1;
	if (left_length > right_length) return 1;
	return 0;
}

static unsigned char *copy_bytes(const void *data, size_t length) {
	unsigned char *copy;

	if (length == SIZE_MAX) return NULL;
	copy = kvstore_malloc(length + 1U);
	if (copy == NULL) return NULL;
	if (length > 0) memcpy(copy, data, length);
	copy[length] = '\0';
	return copy;
}



rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x) {
	while (x->left != T->nil) {
		x = x->left;
	}
	return x;
}

rbtree_node *rbtree_maxi(rbtree *T, rbtree_node *x) {
	while (x->right != T->nil) {
		x = x->right;
	}
	return x;
}

rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->parent;

	if (x->right != T->nil) {
		return rbtree_mini(T, x->right);
	}

	while ((y != T->nil) && (x == y->right)) {
		x = y;
		y = y->parent;
	}
	return y;
}


void rbtree_left_rotate(rbtree *T, rbtree_node *x) {

	rbtree_node *y = x->right;  // x  --> y  ,  y --> x,   right --> left,  left --> right

	x->right = y->left; //1 1
	if (y->left != T->nil) { //1 2
		y->left->parent = x;
	}

	y->parent = x->parent; //1 3
	if (x->parent == T->nil) { //1 4
		T->root = y;
	} else if (x == x->parent->left) {
		x->parent->left = y;
	} else {
		x->parent->right = y;
	}

	y->left = x; //1 5
	x->parent = y; //1 6
}


void rbtree_right_rotate(rbtree *T, rbtree_node *y) {

	rbtree_node *x = y->left;

	y->left = x->right;
	if (x->right != T->nil) {
		x->right->parent = y;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->right) {
		y->parent->right = x;
	} else {
		y->parent->left = x;
	}

	x->right = y;
	y->parent = x;
}

void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {

	while (z->parent->color == RED) { //z ---> RED
		if (z->parent == z->parent->parent->left) {
			rbtree_node *y = z->parent->parent->right;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {

				if (z == z->parent->right) {
					z = z->parent;
					rbtree_left_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_right_rotate(T, z->parent->parent);
			}
		}else {
			rbtree_node *y = z->parent->parent->left;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {
				if (z == z->parent->left) {
					z = z->parent;
					rbtree_right_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_left_rotate(T, z->parent->parent);
			}
		}
		
	}

	T->root->color = BLACK;
}

// int --> char *
int rbtree_insert(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->root;

// strcmp() == 0,  < 0, 

	while (x != T->nil) {
		y = x;
#if ENABLE_KEY_CHAR
		if (compare_bytes(z->key, z->key_length, x->key, x->key_length) < 0) {
			x = x->left;
		} else if (compare_bytes(z->key, z->key_length, x->key, x->key_length) > 0) {
			x = x->right;
		} else {
			return 1;
		}

#else
		if (z->key < x->key) { //strcmp
			x = x->left;
		} else if (z->key > x->key) {
			x = x->right;
		} else { //Exist
			return 1;
		}
#endif
	}

	z->parent = y;
	if (y == T->nil) {
		T->root = z;
#if ENABLE_KEY_CHAR
	} else if (compare_bytes(z->key, z->key_length, y->key, y->key_length) < 0) {
#else
	} else if (z->key < y->key) {
#endif
		y->left = z;
	} else {
		y->right = z;
	}

	z->left = T->nil;
	z->right = T->nil;
	z->color = RED;

	rbtree_insert_fixup(T, z);
	return 0;
}

void rbtree_delete_fixup(rbtree *T, rbtree_node *x) {

	while ((x != T->root) && (x->color == BLACK)) {
		if (x == x->parent->left) {

			rbtree_node *w= x->parent->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;

				rbtree_left_rotate(T, x->parent);
				w = x->parent->right;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rbtree_right_rotate(T, w);
					w = x->parent->right;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbtree_left_rotate(T, x->parent);

				x = T->root;
			}

		} else {

			rbtree_node *w = x->parent->left;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_right_rotate(T, x->parent);
				w = x->parent->left;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rbtree_left_rotate(T, w);
					w = x->parent->left;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbtree_right_rotate(T, x->parent);

				x = T->root;
			}

		}
	}

	x->color = BLACK;
}


// int -->  char *
rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->nil;

	if ((z->left == T->nil) || (z->right == T->nil)) {
		y = z;
	} else {
		y = rbtree_successor(T, z);
	}

	if (y->left != T->nil) {
		x = y->left;
	} else if (y->right != T->nil) {
		x = y->right;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->left) {
		y->parent->left = x;
	} else {
		y->parent->right = x;
	}

	if (y != z) {
		
#if ENABLE_KEY_CHAR
		void *tmp = z->key;
		z->key = y->key;
		y->key = tmp;

		tmp = z->value;
		z->value = y->value;
		y->value = tmp;

		{
			size_t length = z->key_length;
			z->key_length = y->key_length;
			y->key_length = length;
			length = z->value_length;
			z->value_length = y->value_length;
			y->value_length = length;
		}
#else
		z->key = y->key;
		z->value = y->value;
#endif
	}

	if (y->color == BLACK) {
		rbtree_delete_fixup(T, x);
	}

	return y;
}


// int --> char *
rbtree_node *rbtree_search_bytes(rbtree *T, const void *key, size_t key_length) {

	rbtree_node *node = T->root;
	while (node != T->nil) {
		
#if ENABLE_KEY_CHAR
		if (compare_bytes(key, key_length, node->key, node->key_length) < 0) {
			node = node->left;
		} else if (compare_bytes(key, key_length, node->key, node->key_length) > 0) {
			node = node->right;
		} else {
			return node;
		}
#else
		if (key < node->key) {
			node = node->left;
		} else if (key > node->key) {
			node = node->right;
		} else {
			return node;
		}	
#endif
	}
	return T->nil;
}

rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key) {
	return rbtree_search_bytes(T, key, strlen(key));
}


void rbtree_traversal(rbtree *T, rbtree_node *node) {
	if (node != T->nil) {
		rbtree_traversal(T, node->left);
		printf("key:%s, color:%d\n", node->key, node->color);
		rbtree_traversal(T, node->right);
	}
}





// rbtree api
int kvstore_rbtree_create(rbtree *tree) {

	if (!tree) return -1;
	memset(tree, 0, sizeof(rbtree));
	
	tree->nil = (rbtree_node*)kvstore_malloc(sizeof(rbtree_node));
	if (!tree->nil) return -1;
	memset(tree->nil, 0, sizeof(rbtree_node));
	tree->nil->key = kvstore_malloc(1);
	if (!tree->nil->key) {
		kvstore_free(tree->nil);
		tree->nil = NULL;
		return -1;
	}
	*(tree->nil->key) = '\0';
	tree->nil->key_length = 0;
	tree->nil->value_length = 0;
	
	
	tree->nil->color = BLACK;
	tree->nil->left = tree->nil;
	tree->nil->right = tree->nil;
	tree->nil->parent = tree->nil;
	tree->root = tree->nil;
	tree->count = 0;

	return 0;
}

void kvstore_rbtree_destory(rbtree *tree) {

	if (!tree || !tree->nil) return ;

	rbtree_node *node = tree->root;
	while (node != tree->nil) {

		node = rbtree_mini(tree, tree->root);
		if (node == tree->nil) {
			break;
		}

		node = rbtree_delete(tree, node);

		if (node) {
			kvstore_free(node->key);
			kvstore_free(node->value);
			kvstore_free(node);
		}
		

	}
	kvstore_free(tree->nil->key);
	kvstore_free(tree->nil);
	tree->nil = NULL;
	tree->root = NULL;
	tree->count = 0;

}


int kvs_rbtree_set(rbtree *tree, char *key, char *value) {

	int insert_result;
	rbtree_node *node;

	if (!tree || !tree->nil || !key || !value) return -1;
	node  = (rbtree_node*)kvstore_malloc(sizeof(rbtree_node));
	if (!node) return -1;

	node->key_length = strlen(key);
	node->value_length = strlen(value);
	node->key = (char *)copy_bytes(key, node->key_length);
	if (node->key == NULL) {
		kvstore_free(node);
		return -1;
	}
	
	node->value = copy_bytes(value, node->value_length);
	if (node->value == NULL) {
		kvstore_free(node->key);
		kvstore_free(node);
		return -1;
	}

	insert_result = rbtree_insert(tree, node);
	if (insert_result != 0) {
		kvstore_free(node->key);
		kvstore_free(node->value);
		kvstore_free(node);
		return insert_result;
	}
	tree->count ++;

	return 0;
}

char* kvs_rbtree_get(rbtree *tree, char *key) {
	if (!tree || !tree->nil || !key) return NULL;

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return NULL;
	}

	return node->value;
	
}


int kvs_rbtree_delete(rbtree *tree, char *key) {
	if (!tree || !tree->nil || !key) return -1;

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return -1;
	}
	
	rbtree_node *cur = rbtree_delete(tree, node);

	if (cur) {
		kvstore_free(cur->key);
		kvstore_free(cur->value);
		kvstore_free(cur);
	}
	tree->count --;
	
	return 0;
}


int kvs_rbtree_modify(rbtree *tree, char *key, char *value) {
	char *replacement;

	if (!tree || !tree->nil || !key || !value) return -1;

	rbtree_node *node = rbtree_search(tree, key);
	if (node == tree->nil) {
		return -1;
	}

	replacement = kvstore_malloc(strlen(value) + 1);
	if (replacement == NULL) {
		return -1;
	}
	strcpy(replacement, value);
	kvstore_free(node->value);
	node->value = replacement;
	node->value_length = strlen(value);

	return 0;
}

int kvs_rbtree_count(rbtree *tree) {

	return tree ? tree->count : -1;

}

int kvs_rbtree_upsert_bytes(rbtree *tree,
							const void *key,
							size_t key_length,
							const void *value,
							size_t value_length) {
	rbtree_node *node;
	unsigned char *replacement;
	int insert_result;

	if (!tree || !tree->nil || !valid_bytes(key, key_length) ||
		!valid_bytes(value, value_length)) return -1;
	node = rbtree_search_bytes(tree, key, key_length);
	if (node != tree->nil) {
		replacement = copy_bytes(value, value_length);
		if (replacement == NULL) return -1;
		kvstore_free(node->value);
		node->value = replacement;
		node->value_length = value_length;
		return 0;
	}

	node = kvstore_malloc(sizeof(*node));
	if (node == NULL) return -1;
	memset(node, 0, sizeof(*node));
	node->key = (char *)copy_bytes(key, key_length);
	if (node->key == NULL) {
		kvstore_free(node);
		return -1;
	}
	node->value = copy_bytes(value, value_length);
	if (node->value == NULL) {
		kvstore_free(node->key);
		kvstore_free(node);
		return -1;
	}
	node->key_length = key_length;
	node->value_length = value_length;
	insert_result = rbtree_insert(tree, node);
	if (insert_result != 0) {
		kvstore_free(node->key);
		kvstore_free(node->value);
		kvstore_free(node);
		return -1;
	}
	tree->count++;
	return 0;
}

const void *kvs_rbtree_get_bytes(rbtree *tree,
								 const void *key,
								 size_t key_length,
								 size_t *value_length) {
	rbtree_node *node;

	if (!tree || !tree->nil || value_length == NULL ||
		!valid_bytes(key, key_length)) return NULL;
	*value_length = 0;
	node = rbtree_search_bytes(tree, key, key_length);
	if (node == tree->nil) return NULL;
	*value_length = node->value_length;
	return node->value;
}

int kvs_rbtree_delete_bytes(rbtree *tree,
							const void *key,
							size_t key_length) {
	rbtree_node *node;
	rbtree_node *removed;

	if (!tree || !tree->nil || !valid_bytes(key, key_length)) return -1;
	node = rbtree_search_bytes(tree, key, key_length);
	if (node == tree->nil) return 0;
	removed = rbtree_delete(tree, node);
	if (removed != NULL) {
		kvstore_free(removed->key);
		kvstore_free(removed->value);
		kvstore_free(removed);
	}
	tree->count--;
	return 1;
}


rbtree Tree;







#if 0


int main() {
#if ENABLE_KEY_CHAR

	KEY_TYPE keyArray[10] = {
		"King", "Darren", "Mark", "Vico", "Nick", "cb", "1323", "Zvoice", "LingSheng", "Youzi"
	};

	char *valueArray[10] = {
		"king", "darren", "mark", "vico", "nick", "cb", "1323", "zvoce", "lingsheng", "youzi"
	};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}

	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->key = malloc(1);
	*(T->nil->key) = '\0';
	
	
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;

	int i = 0;
	for (i = 0;i < 10;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		
		node->key = malloc(strlen(keyArray[i]) + 1);
		memset(node->key, 0, strlen(keyArray[i]) + 1);
		strcpy(node->key, keyArray[i]);
		
		node->value = malloc(strlen(valueArray[i]) + 1);
		memset(node->value, 0, strlen(valueArray[i]) + 1);
		strcpy((char *)node->value, keyArray[i]);

		rbtree_insert(T, node);

	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 10;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);

		if (!cur) {
			free(cur->key);
			free(cur->value);
			free(cur);
		}

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}

#else
	int keyArray[20] = {24,25,13,35,23, 26,67,47,38,98, 20,19,17,49,12, 21,9,18,14,15};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}
	
	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;
	int i = 0;
	for (i = 0;i < 20;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		node->key = keyArray[i];
		node->value = NULL;

		rbtree_insert(T, node);
		
	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 20;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);
		free(cur);

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}
	
#endif

	
}

#endif


