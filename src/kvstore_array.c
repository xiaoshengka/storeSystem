
#include "kvstore.h"

//struct kvs_array_item array_table[KVS_ARRAY_SIZE] = {0};
//int array_idx = 0;

array_t Array;

// create
int kvstore_array_create(array_t *arr){
  if(!arr) return -1;
  
  arr->array_table = kvstore_malloc(KVS_ARRAY_SIZE *sizeof(struct kvs_array_item));
  if(!arr->array_table){
    return -1;
  }
  memset(arr->array_table, 0, KVS_ARRAY_SIZE *sizeof(struct kvs_array_item));
  
  arr->array_idx = 0;
  
  return 0;
}

// destory
void kvstore_array_destory(array_t *arr){
  if(!arr) return ;

  if(arr->array_table) {
    int i;
    for(i = 0; i < arr->array_idx; ++i) {
      kvstore_free(arr->array_table[i].key);
      kvstore_free(arr->array_table[i].value);
    }
    kvstore_free(arr->array_table);
  }
  arr->array_table = NULL;
  arr->array_idx = 0;
}



int kvs_array_set(array_t *arr, char *key, char *value){

  if(arr == NULL || key == NULL || value == NULL) return -1;
  if(arr->array_idx == KVS_ARRAY_SIZE) return -1;
  
  int i;
  char *kcopy;
  char *vcopy;

  for(i = 0; i < arr->array_idx; ++i) {
    if(strcmp(arr->array_table[i].key, key) == 0) return 1;
  }

  kcopy = kvstore_malloc(strlen(key)+1);
  if(kcopy == NULL) return -1;
  strcpy(kcopy, key);
  
  vcopy = kvstore_malloc(strlen(value)+1);
  if(vcopy == NULL) {
    kvstore_free(kcopy);
    return -1;
  }
  strcpy(vcopy, value);

  arr->array_table[arr->array_idx].key = kcopy;
  arr->array_table[arr->array_idx].value = vcopy;
  arr->array_idx++;

  return 0;
}

char *kvs_array_get(array_t *arr, char *key){ 
  if(arr == NULL || key == NULL) return NULL;
  int i=0;
  for(i=0; i<arr->array_idx; i++){
    if(strcmp(arr->array_table[i].key, key) == 0){
      return arr->array_table[i].value;
    }
  }
  
  return NULL;

}

int kvs_array_del(array_t *arr, char *key){ 
  if(arr == NULL || key == NULL) return -1;
  
  int i=0;
  for(i=0; i<arr->array_idx; i++){
    if(strcmp(arr->array_table[i].key, key) == 0){
      kvstore_free(arr->array_table[i].value);
      arr->array_table[i].value = NULL;
      
      kvstore_free(arr->array_table[i].key);
      arr->array_table[i].key = NULL;
      if(i != arr->array_idx - 1) {
        arr->array_table[i] = arr->array_table[arr->array_idx - 1];
      }
      arr->array_table[arr->array_idx - 1].key = NULL;
      arr->array_table[arr->array_idx - 1].value = NULL;
      arr->array_idx--;
      
      return 0;
    }
  }
  
  return 1;    // no exist

}

int kvs_array_mod(array_t *arr, char *key, char *value){
  if(arr == NULL || key == NULL || value == NULL) return -1;
  
  int i=0;
  for(i=0; i<arr->array_idx; i++){
    if(strcmp(arr->array_table[i].key, key) == 0){
      char *vcopy = kvstore_malloc(strlen(value)+1);
      if(vcopy == NULL) return -1;
      strcpy(vcopy, value);
      
      kvstore_free(arr->array_table[i].value);
      arr->array_table[i].value = vcopy;
      
      return 0;
    }
  }
  
  return 1;
}


int kvs_array_count(array_t *arr){
  if(arr == NULL) return -1;
  return arr->array_idx;
} 




