#include "../../config.h"
#include "../../globals.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <float.h>
#include "../../include/ads/pqueueGP.h"



struct IsaxPQ * initializeQueue(long int max_size , struct IsaxPQ *queue)
{	
    queue = (struct IsaxPQ*)malloc(sizeof(struct IsaxPQ));
    queue -> size = 0;
    queue -> isaxPQ = (query_result**)malloc(sizeof(query_result*) * max_size);
    pthread_mutex_init(&queue->lock, NULL);
    return queue;
}

int deleteMin(struct IsaxPQ *queue){

    query_result *u  = NULL; //root node
    query_result *w = NULL; //child node with smallest key
	query_result *rightchild  = NULL;
	query_result *leftChild = NULL;
	
    int toDelPos = 0;
	int childPos = 0;

	if(queue ->size == 0){
		return 1;
	}
	u = queue->isaxPQ[toDelPos];

    if (2 * toDelPos + 1< queue->size) {
		leftChild = queue->isaxPQ[2 * toDelPos + 1];
	}
	if (2 * toDelPos + 2 < queue->size) {
		rightchild = queue->isaxPQ[2 * toDelPos + 2];
	}
	
	if (rightchild == NULL || leftChild->distance < rightchild->distance)
	{
		w = queue->isaxPQ[2 * toDelPos + 1];
		childPos = 2 * toDelPos + 1;
	}
	else {
		w = queue->isaxPQ[2 * toDelPos + 2];
		childPos = 2 * toDelPos + 2;
	}

    while (1)
	{


		if (u == NULL || u->distance <= w->distance) {
			return 1;
		}
		query_result *temp = u; // antallagh
		queue->isaxPQ[toDelPos] = w;
		queue->isaxPQ[childPos] = temp;
		toDelPos = childPos;
		if (2 * toDelPos + 1< queue->size) {
			leftChild = queue->isaxPQ[2 * toDelPos + 1];
		}
		if (2 * toDelPos + 2 < queue->size) {
			rightchild = queue->isaxPQ[2 * toDelPos + 2];
		}
		if (leftChild->distance<rightchild->distance)
		{
			w = queue->isaxPQ[2 * toDelPos + 1];
			if (2 * toDelPos + 1 < queue->size) {
				childPos = 2 * toDelPos + 1;
			}
		}
		else {
			w = queue->isaxPQ[2 * toDelPos + 2];
			if (2 * toDelPos + 2 < queue->size)
			{
				childPos = 2 * toDelPos + 2;
			}

		}
		u = queue->isaxPQ[toDelPos];
		w = queue->isaxPQ[childPos];

	}
}

float get_minIsaxResult(struct IsaxPQ *queue) {
	
	query_result *minIsax;
	pthread_mutex_lock(&queue->lock);
	if (queue == NULL || queue->size == 0) {
		pthread_mutex_unlock(&queue->lock);
		return -1;
	}
	minIsax = queue->isaxPQ[0];
	float min_isax = minIsax->distance;
	queue->isaxPQ[0] = queue->isaxPQ[queue->size - 1];
	queue->size--;
	deleteMin(queue);
	pthread_mutex_unlock(&queue->lock);
	return min_isax;
}

int insertIsaxResult( query_result* tmp , struct IsaxPQ *queue)
{
	pthread_mutex_lock(&queue->lock);
	if (queue->size == 0) {

		queue->isaxPQ[queue->size] = tmp;//size=0
		queue->size++;
		pthread_mutex_unlock(&queue->lock);
		return 1;
	}

	int parent_pos = (queue->size - 1) / 2;
	int curr_pos = queue->size;


	query_result *parent = queue->isaxPQ[parent_pos];

	query_result *newNode = tmp;
	queue->isaxPQ[queue->size] = newNode;//size>0
	queue->size++;
	while(1) {
	
		if (parent == NULL || parent->distance< newNode->distance) {
			pthread_mutex_unlock(&queue->lock);
			return 1;
		}
		queue->isaxPQ[curr_pos] = parent; //Antalasoume ta dedomena tou kombou me tou patrikou
		queue->isaxPQ[parent_pos] = newNode;
		curr_pos = parent_pos;
		newNode = queue->isaxPQ[curr_pos];
		if (curr_pos == 0) {
			parent = NULL;
			continue;
		}
		parent_pos = (parent_pos - 1) / 2; // Briskomai ton neo patriko kombo
		parent = queue->isaxPQ[parent_pos];
    }	
}