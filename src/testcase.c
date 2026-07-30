
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>

#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define MAX_MAS_LENGTH 512

int send_msg(int connfd, char *msg, int length){
  int res = send(connfd, msg, length, 0);
  if(res < 0){
    perror("send");
    exit(1);
  }
  return res;
}

int recv_msg(int connfd, char *msg, int length){
  int res = recv(connfd, msg, length, 0);
  if(res < 0){
    perror("recv");
    exit(1);
  }
  return res;
}

void equals(char *pattern, char *result, char *casename){
  if(strcmp(pattern, result) == 0){
    //printf("==> PASS --> %s\n", casename);
  }else{
    printf("==> FAILED --> %s, '%s' != '%s'\n", casename, pattern, result);
  }
}


void test_case(int connfd, char *msg, char *pattern, char *casename){
  if(!msg || !pattern || !casename) return ;

  send_msg(connfd, msg, strlen(msg));
  
  char result[MAX_MAS_LENGTH] = {0};
  recv_msg(connfd, result, MAX_MAS_LENGTH);
  
  equals(pattern, result, casename);
}

void array_testcase(int connfd){
  test_case(connfd, "SET Name ZHK", "SUCCESS", "SETCase");
  test_case(connfd, "GET Name", "ZHK", "GETCase");
  test_case(connfd, "MOD Name MYY", "SUCCESS", "MODCase");
  test_case(connfd, "GET Name", "MYY", "GETCase");
  test_case(connfd, "DEL Name", "SUCCESS", "DELCase");
  test_case(connfd, "GET Name", "NO EXIST", "GETCase");
}

void array_testcase_10w(int connfd){
  int count = 100000;
  int i=0;
  while(i++ < count){
    array_testcase(connfd);
  }
}


void rbtree_testcase(int connfd){
  test_case(connfd, "RSET Name ZHK", "SUCCESS", "SETCase");
  test_case(connfd, "RGET Name", "ZHK", "GETCase");
  test_case(connfd, "RMOD Name MYY", "SUCCESS", "MODCase");
  test_case(connfd, "RGET Name", "MYY", "GETCase");
  test_case(connfd, "RDEL Name", "SUCCESS", "DELCase");
  test_case(connfd, "RGET Name", "NO EXIST", "GETCase");
}

void rbtree_testcase_10w(int connfd){
  int count = 100000;
  int i=0;
  while(i++ < count){
    rbtree_testcase(connfd);
  }
}

void rbtree_testcase_5w_node(int connfd){
  int count = 50000;
  int i=0;
  for(i=0; i<count; i++){
    char cmd[128] = {0};
    snprintf(cmd, 128, "RSET Name%d ZHK%d", i, i);
    test_case(connfd, cmd, "SUCCESS", "SETCase");
    
    char result[128] = {0};
    sprintf(result, "%d", i+1);
    test_case(connfd, "RCOUNT", result, "RCOUNTCase");
  }
  
  for(i=0; i<count; i++){
    char cmd[128] = {0};
    snprintf(cmd, 128, "RDEL Name%d ZHK%d", i, i);
    test_case(connfd, cmd, "SUCCESS", "DELCase");
    
    char result[128] = {0};
    sprintf(result, "%d", count-(i+1));
    test_case(connfd, "RCOUNT", result, "RCOUNTCase");
  }
}


void hash_testcase(int connfd){
  test_case(connfd, "HSET Name ZHK", "SUCCESS", "SETCase");
  test_case(connfd, "HGET Name", "ZHK", "GETCase");
  test_case(connfd, "HMOD Name MYY", "SUCCESS", "MODCase");
  test_case(connfd, "HGET Name", "MYY", "GETCase");
  test_case(connfd, "HDEL Name", "SUCCESS", "DELCase");
  test_case(connfd, "HGET Name", "NO EXIST", "GETCase");
}

void hash_testcase_10w(int connfd){
  int count = 100000;
  int i=0;
  while(i++ < count){
    hash_testcase(connfd);
  }
}

void hash_testcase_5w_node(int connfd){
  int count = 50000;
  int i=0;
  for(i=0; i<count; i++){
    char cmd[128] = {0};
    snprintf(cmd, 128, "HSET Name%d ZHK%d", i, i);
    test_case(connfd, cmd, "SUCCESS", "SETCase");
    
    char result[128] = {0};
    sprintf(result, "%d", i+1);
    test_case(connfd, "HCOUNT", result, "HCOUNTCase");
  }
  
  for(i=0; i<count; i++){
    char cmd[128] = {0};
    snprintf(cmd, 128, "HDEL Name%d ZHK%d", i, i);
    test_case(connfd, cmd, "SUCCESS", "DELCase");
    
    char result[128] = {0};
    sprintf(result, "%d", count-(i+1));
    test_case(connfd, "HCOUNT", result, "HCOUNTCase");
  }
}


int connect_tcpserver(const char *ip, unsigned short port){
  
  int connfd = socket(AF_INET, SOCK_STREAM, 0);
  
  struct sockaddr_in tcpserver_addr;
  memset(&tcpserver_addr, 0, sizeof(struct sockaddr_in));
  
  tcpserver_addr.sin_family = AF_INET;
  tcpserver_addr.sin_addr.s_addr = inet_addr(ip);
  tcpserver_addr.sin_port = htons(port);
  
  int ret = connect(connfd, (struct sockaddr*)&tcpserver_addr, sizeof(struct sockaddr_in));
  if(ret){
    perror("connect\n");
    return -1;
  }
  
  return connfd;
}


// array:0x01, rbtree:0x02, hash:0x04, skiptable:0x08

// ./testcase -s 192.168.232.129 -p 9096 -m 1
int main(int argc, char *argv[])
{
  int ret = 0;
  
  char ip[16] = {0};
  int port = 0;
  int mode = 1;

  int opt;
  while((opt = getopt(argc, argv, "s:p:m:?")) != -1){
    switch(opt){
      case 's':
        strcpy(ip, optarg);
        break;
        
      case 'p':
        port = atoi(optarg);
        break;
        
      case 'm':
        mode = atoi(optarg);
        break;
        
      default:
        return -1;
    }
  }
  
  int connfd = connect_tcpserver(ip, port);
  
  if(mode & 0x01){    // array
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
  
    //array_testcase(connfd);
    array_testcase_10w(connfd);
    
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("array time_used: %d, qps: %d\n", time_used, (100000*6)*1000 / time_used);
  }
  
  if(mode & 0x02){    // rbtree
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
  
    //rbtree_testcase(connfd);
    //rbtree_testcase_10w(connfd);
    rbtree_testcase_5w_node(connfd);
    
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("rbtree time_used: %d, qps: %d\n", time_used, 200000*1000 / time_used);
  }
  
  if(mode & 0x04){    // hash
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
  
    //hash_testcase(connfd);
    //hash_testcase_10w(connfd);
    hash_testcase_5w_node(connfd);
    
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    //printf("hash time_used: %d, qps: %d\n", time_used, (100000*6)*1000 / time_used);
    printf("hash time_used: %d, qps: %d\n", time_used, 200000*1000 / time_used);
  }
  
  if(mode & 0x08){
    struct timeval tv_begin;
    gettimeofday(&tv_begin, NULL);
  
    rbtree_testcase(connfd);
    //rbtree_testcase_10w(connfd);
    
    struct timeval tv_end;
    gettimeofday(&tv_end, NULL);
    
    int time_used = TIME_SUB_MS(tv_end, tv_begin);
    printf("skiptable time_used: %d, qps: %d\n", time_used, (100000*6)*1000 / time_used);
  }
  
  
}


