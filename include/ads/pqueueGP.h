#include "../../config.h"
#include "../../globals.h"
#include "isax_query_engine.h"
#include "isax_node.h"
#ifndef PQUEUEGP_H
#define PQUEUEGP_H

/*---------------------------------------------Geopat pqueue---------------------------------------------------------------*/

struct IsaxPQ{
	long int size; /* Current size of the priority queue. Update accordingly when inserting/deleting nodes */
	query_result **isaxPQ; /* Dynamically allocated array (once at startup) to implement the priority queue as a heap. */
    pthread_mutex_t lock;
};

struct IsaxPQ * initializeQueue(long int max_size , struct IsaxPQ *queue);

int deleteMin(struct IsaxPQ *queue);

query_result * get_minIsaxResult(struct IsaxPQ *queue);

int insertIsaxResult(query_result* tmp , struct IsaxPQ *queue);

int freeQueue(struct IsaxPQ *queue);

int getSize(struct IsaxPQ *queue);
int print_queue(struct IsaxPQ *queue);

#endif