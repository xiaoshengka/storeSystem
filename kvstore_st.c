
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <limits.h>

#include "kvstore.h"

#define MAX_LEVEL 6


Node* createNode(int level, int key){
  Node *newNode = (Node*)malloc(sizeof(Node));
  newNode->key = key;
  newNode->forward = (Node**)malloc(sizeof(Node*) * (level+1));
  for(int i=0; i<= level; ++i){
    newNode->forward[i] = NULL;
  }
  return newNode;
}

SkipList* createSkipList(skiptable_t* skipList){
  if(skipList == NULL) return -1;
  //SkipList* skipList = (SkipList*)malloc(sizeof(SkipList));
  skipList->level = 0;
  skipList->header = createNode(MAX_LEVEL, INT_MIN);
  
  return skipList;
}

int randomLevel(){
  int level = 0;
  
  while(rand() < RAND_MAX/2 && level < MAX_LEVEL){
    level++;
  }
  
  return level;
}

void insert(SkipList* skipList, int key){
  Node* update[MAX_LEVEL + 1];
  Node* current = skipList->header;
  
  for(int i=skipList->level; i>=0; i--){
    while(current->forward[i] != NULL && current->forward[i]->key < key){
      current = current->forward[i];
    }
    update[i] = current;
  }
  current = current->forward[0];
  
  if(current == NULL || current->key != key){
    int level = randomLevel();
    if(level > skipList->level){
      for(int i=skipList->level+1; i<=level; ++i){
        update[i] = skipList->header;
      }
      skipList->level = level;
    }
    
    Node* newNode = createNode(level, key);
    for(int i=0; i<=level; ++i){
      newNode->forward[i] = update[i]->forward[i];
      update[i]->forward[i] = newNode;
    }
    
    printf("Success, key: %d\n", key);
  }

}

bool search(SkipList* skipList, int key){
  Node* current = skipList->header;
  for(int i=skipList->level; i>=0; --i){
    while(current->forward[i] != NULL && current->forward[i]->key < key){
      current = current->forward[i];
    }
  }
  
  current = current->forward[0];
  
  if(current != NULL && current->key == key){
    printf("key %d found\n", key);
    return true;
  }
  
  printf("key %d not found\n", key);
    return false;
}

void deleteNode(SkipList* skipList, int key){
  Node* update[MAX_LEVEL+1];
  Node* current = skipList->header;
  
  for(int i=skipList->level; i>=0; --i){
    while(current->forward[i] != NULL && current->forward[i]->key < key){
      current = current->forward[i];
    }
    update[i] = current;
  }
  
  current = current->forward[0];
  
  if(current != NULL && current->key == key){
    for(int i=0; i<=skipList->level; ++i){
      if(update[i]->forward[i] != current){
        break;
      }
      update[i]->forward[i] = current->forward[i];
    }
    free(current->forward);
    free(current);
    
    while(skipList->level > 0 && skipList->header->forward[skipList->level] == NULL){
      skipList->level--;
    }
    
    printf("Success delete key %d\n", key);
  }
}

void printSkipList(SkipList* skipList){
  printf("\nSkipList\n");
  for(int i=0; i<=skipList->level; ++i){
    Node* node = skipList->header->forward[i];
    printf("Level %d: ", i);
    while(node != NULL){
      printf("%d ", node->key);
      node = node->forward[i];
    }
    printf("\n");
  }
}

// 5+2

skiptable_t skiptable;

int kvstore_skiptable_create(skiptable_t *table){
  return createSkipList(table);
}

void kvstore_skiptable_destory(skiptable_t *table){
  
}



#if 0
int main(){
  SkipList* skipList = createSkipList();
  
  insert(skipList, 3);
  insert(skipList, 4);
  insert(skipList, 5);
  insert(skipList, 6);
  insert(skipList, 7);
  insert(skipList, 8);
  insert(skipList, 9);
  
  printSkipList(skipList);
  
  search(skipList, 6);
  
  deleteNode(skipList, 5);
  
  printSkipList(skipList);
  
  return 0;
}

#endif






