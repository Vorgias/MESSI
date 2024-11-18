
#define _GNU_SOURCE


#ifdef VALUES
#include <values.h>
#endif
#include <float.h>
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/wait.h>
#include <sched.h>
#include <unistd.h>


#include "omp.h"  
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include "ads/inmemory_topk_engine.h"
#include "ads/pqueueGP.h"
//LF QUEUE
#include "ads/prioq.h"
#include "gc/gc.h"

#define left(i)   ((i) << 1)
#define right(i)  (((i) << 1) + 1)
#define parent(i) ((i) >> 1)

int NUM_PRIORITY_QUEUES;

inline void threadPin(int pid, int max_threads) {
    int cpu_id;

    cpu_id = pid % max_threads;
    pthread_setconcurrency(max_threads);

    cpu_set_t mask;  
    unsigned int len = sizeof(mask);

    CPU_ZERO(&mask);

    // CPU_SET(cpu_id % max_threads, &mask);                              // OLD PINNING 1

    // if (cpu_id % 2 == 0)                                             // OLD PINNING 2
    //    CPU_SET(cpu_id % max_threads, &mask);
    // else 
    //    CPU_SET((cpu_id + max_threads/2)% max_threads, &mask);

    // if (cpu_id % 2 == 0)                                             // FULL HT
    //    CPU_SET(cpu_id/2, &mask);
    // else 
    //    CPU_SET((cpu_id/2) + (max_threads/2), &mask);

    CPU_SET((cpu_id%4)*10 + (cpu_id%40)/4 + (cpu_id/40)*40, &mask);     // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}


// -------------------------------------
// -------------------------------------

// Botao's version
query_result exact_search_ParISnew_inmemory_hybrid (ts_type *ts, ts_type *paa, isax_index *index,node_list *nodelist,
                           float minimum_distance, int min_checked_leaves) 
{   
     //   RDcalculationnumber=0;
    //LBDcalculationnumber=0;
    query_result approximate_result = approximate_search_inmemory_pRecBuf(ts, paa, index);
    //query_result approximate_result = approximate_search_inmemory(ts, paa, index);
    query_result bsf_result = approximate_result;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    int node_counter=0;
    // Early termination...
    if (approximate_result.distance == 0) {
        return approximate_result;
    }

    // EKOSMAS: REMOVED, 01 SEPTEMBER 2020
    // if(approximate_result.distance == FLT_MAX || min_checked_leaves > 1) {
    //     approximate_result = refine_answer_inmemory_m(ts, paa, index, approximate_result, minimum_distance, min_checked_leaves);
    // }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }

    pqueue_t **allpq=malloc(sizeof(pqueue_t*)*NUM_PRIORITY_QUEUES);


    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    int queuelabel[NUM_PRIORITY_QUEUES];

    query_result *do_not_remove = &approximate_result;

    SET_APPROXIMATE(approximate_result.distance);


    if(approximate_result.node != NULL) {
        // Insert approximate result in heap.
        //pqueue_insert(pq, &approximate_result);
        //GOOD: if(approximate_result.node->filename != NULL)
        //GOOD: printf("POPS: %.5lf\t", approximate_result.distance);
    }
    // Insert all root nodes in heap.
    isax_node *current_root_node = index->first_node;

    pthread_t threadid[maxquerythread];
    MESSI_workerdata workerdata[maxquerythread];
    pthread_mutex_t lock_queue=PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t lock_current_root_node=PTHREAD_MUTEX_INITIALIZER;
    pthread_rwlock_t lock_bsf=PTHREAD_RWLOCK_INITIALIZER;
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);
 
    
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
                allpq[i]=pqueue_init(index->settings->root_nodes_size/NUM_PRIORITY_QUEUES,
                               cmp_pri, get_pri, set_pri, get_pos, set_pos);
                pthread_mutex_init(&ququelock[i], NULL);
                queuelabel[i]=1;
    }

    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].paa=paa;
        workerdata[i].ts=ts;
        workerdata[i].lock_queue=&lock_queue;                            
        workerdata[i].lock_current_root_node=&lock_current_root_node;
        workerdata[i].lock_bsf=&lock_bsf;
        workerdata[i].nodelist= nodelist->nlist;
        workerdata[i].amountnode=nodelist->node_amount;
        workerdata[i].index=index;
        workerdata[i].minimum_distance=minimum_distance;
        workerdata[i].node_counter=&node_counter;
        workerdata[i].pq=allpq[i];
        workerdata[i].bsf_result = &bsf_result;
        workerdata[i].lock_barrier=&lock_barrier;
        workerdata[i].alllock=ququelock;
        workerdata[i].allqueuelabel=queuelabel;
        workerdata[i].allpq=allpq;
        workerdata[i].startqueuenumber=i%NUM_PRIORITY_QUEUES;
        workerdata[i].workernumber=i;                                       // EKOSMAS, AUGUST 29 2020: Added
    }
        
    
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]),NULL,exact_search_worker_inmemory_hybridpqueue,(void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i],NULL);
    }

    // Free the nodes that where not popped.
    // Free the priority queue.
    pthread_barrier_destroy(&lock_barrier);

    //pqueue_free(pq);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);
    bsf_result=bsf_result;

    //free(rfdata);
     //       printf("the number of LB distance calculation is %ld\t\t and the Real distance calculation is %ld\n ",LBDcalculationnumber,RDcalculationnumber);
    return bsf_result;

    // Free the nodes that where not popped.
}
// Botao's version
void* exact_search_worker_inmemory_hybridpqueue(void *rfdata)
{   
    threadPin(((MESSI_workerdata*)rfdata)->workernumber, maxquerythread);           // EKOSMAS, AUGUST 29 2020: Added

    isax_node *current_root_node;
    query_result *n;
    isax_index *index=((MESSI_workerdata*)rfdata)->index;
    ts_type *paa=((MESSI_workerdata*)rfdata)->paa;
    ts_type *ts=((MESSI_workerdata*)rfdata)->ts;
    pqueue_t *pq=((MESSI_workerdata*)rfdata)->pq;
    query_result *do_not_remove = ((MESSI_workerdata*)rfdata)->bsf_result;
    float minimum_distance=((MESSI_workerdata*)rfdata)->minimum_distance;
    int limit=((MESSI_workerdata*)rfdata)->limit;
    int checks = 0;
    bool finished=true;
    int current_root_node_number;
    int tight_bound = index->settings->tight_bound;
    int aggressive_check = index->settings->aggressive_check;
    query_result *bsf_result=(((MESSI_workerdata*)rfdata)->bsf_result);
    float bsfdisntance=bsf_result->distance;
    int calculate_node=0,calculate_node_quque=0;
    int tnumber=rand()% NUM_PRIORITY_QUEUES;
    int startqueuenumber=((MESSI_workerdata*)rfdata)->startqueuenumber;
    //COUNT_QUEUE_TIME_START

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) 
    {
            current_root_node_number=__sync_fetch_and_add(((MESSI_workerdata*)rfdata)->node_counter,1);
            //printf("the number is %d\n",current_root_node_number );
            if(current_root_node_number>= ((MESSI_workerdata*)rfdata)->amountnode)
            break;
            current_root_node=((MESSI_workerdata*)rfdata)->nodelist[current_root_node_number];

            insert_tree_node_m_hybridpqueue(paa,current_root_node,index,bsfdisntance,((MESSI_workerdata*)rfdata)->allpq,((MESSI_workerdata*)rfdata)->alllock,&tnumber);
            //insert_tree_node_mW(paa,current_root_node,index,bsfdisntance,pq,((MESSI_workerdata*)rfdata)->lock_queue);

            
    }

    //COUNT_QUEUE_TIME_END
    //calculate_node_quque=pq->size;

    pthread_barrier_wait(((MESSI_workerdata*)rfdata)->lock_barrier);
    //printf("the size of quque is %d \n",pq->size);

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    while (1)
    {
        pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[startqueuenumber]);
        pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        if(n==NULL)
            break;
        //pthread_rwlock_rdlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        bsfdisntance=bsf_result->distance;
        //pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
        // The best node has a worse mindist, so search is finished!

        if (n->distance > bsfdisntance || n->distance > minimum_distance) {
            break;
        }
        else 
        {
            // If it is a leaf, check its real distance.
            if (((isax_node*)n->node)->is_leaf) {

                checks++;

                float distance = calculate_node_distance2_inmemory(index, n->node, ts,paa, bsfdisntance);
                if (distance < bsfdisntance)
                {
                    pthread_rwlock_wrlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                    if(distance < bsf_result->distance)
                    {
                        bsf_result->distance = distance;
                        bsf_result->node = n->node;
                    }
                    pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                }

            }
            
        }
            free(n);
    }

    if( (((MESSI_workerdata*)rfdata)->allqueuelabel[startqueuenumber])==1)
    {
        (((MESSI_workerdata*)rfdata)->allqueuelabel[startqueuenumber])=0;
        pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
        while(n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[startqueuenumber]));
    }

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    while(1)
    {   
        int offset=rand()% NUM_PRIORITY_QUEUES;
        finished=true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if((((MESSI_workerdata*)rfdata)->allqueuelabel[i])==1)
            {
                finished=false;
                while(1)
                {
                    pthread_mutex_lock(&(((MESSI_workerdata*)rfdata)->alllock[i]));
                    n = pqueue_pop(((MESSI_workerdata*)rfdata)->allpq[i]);
                    pthread_mutex_unlock(&(((MESSI_workerdata*)rfdata)->alllock[i]));
                    if(n==NULL)
                    break;
                    if (n->distance > bsfdisntance || n->distance > minimum_distance) {
                        break;
                    }        
                    else 
                    {
                        // If it is a leaf, check its real distance.
                        if (((isax_node*)n->node)->is_leaf) 
                        {
                            checks++;
                            float distance = calculate_node_distance2_inmemory(index, n->node, ts,paa, bsfdisntance);
                            if (distance < bsfdisntance)
                            {
                                pthread_rwlock_wrlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                                if(distance < bsf_result->distance)
                                {
                                    bsf_result->distance = distance;
                                    bsf_result->node = n->node;
                                }
                                pthread_rwlock_unlock(((MESSI_workerdata*)rfdata)->lock_bsf);
                            }

                        }
            
                    }
                //add
                free(n);
                }

            }
        }
        if (finished)
        {
            break;
        }
    }

    if (((MESSI_workerdata*)rfdata)->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    //pthread_barrier_wait(((MESSI_workerdata*)rfdata)->lock_barrier);
    //while(n=pqueue_pop(pq))
    //{
            //free(n);
    //}
    //pqueue_free(pq);
    //
    

                                      //printf("create pq time is %f \n",worker_total_time );
    //printf("the check's node is\t %d\tthe local queue's node is\t%d\n",checks,calculate_node_quque);
}
// Botao's version
void insert_tree_node_m_hybridpqueue(float *paa,isax_node *node,isax_index *index,float bsf,pqueue_t **pq,pthread_mutex_t *lock_queue,int *tnumber)
{   
    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function? I wait a coorect version of it by Botao
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END

    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance=distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
            added_tree_node++;
        }
        else
        {   
            if (((isax_node*)(node->left_child))->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa,((isax_node*)node->left_child),index, bsf,pq,lock_queue,tnumber);
            }
            if (((isax_node*)node->right_child)->isax_cardinalities != NULL)
            {
                insert_tree_node_m_hybridpqueue(paa,(isax_node*)node->right_child,index,bsf,pq,lock_queue,tnumber);
            }
        }
    }
}


// -------------------------------------
// -------------------------------------

////////////////////////////////////////////////////////////
query_result exact_search_ParISnew_inmemory_hybrid_vorgias(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance,attribute_type*attribute) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_vorgias(ts, paa, index,attribute);//////////////
    SET_APPROXIMATE(bsf_result.distance);
    printf("Bsf Result %f\n",bsf_result.distance);
    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t*)*NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES];                                            // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER;                   // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size/NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i]=1;                                                 
    }

    volatile int node_counter = 0;                                              // EKOSMAS, 29 AUGUST 2020: added volatile

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].lock_bsf = &lock_bsf;                                     // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                                 // the BSF
        workerdata[i].lock_barrier = &lock_barrier;                             // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                                      // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                               // ??? How is this used?         
        workerdata[i].allpq = allpq;                                            // priority queues
        workerdata[i].startqueuenumber = i%NUM_PRIORITY_QUEUES;                            // initial priority queue to start
        workerdata[i].workernumber = i;
        /////////////////////////////////////////////
        workerdata[i].attribute = attribute;
        /////////////////////////////////////////////
    }
        
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_vorgias, (void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    pthread_barrier_destroy(&lock_barrier);

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);
    if(bsf_result.record!=NULL){
        for(int i=0;i<index->settings->attribute_size;i++){
            printf("\nResult has attribute: (%d) %d\n",i,bsf_result.record->attr[i]);
        }
    //printf("Result has attribute : %d\n",*(bsf_result.record->attr));
    }
    return bsf_result;

    // Free the nodes that where not popped.
}
///////////////////////////////////////////////////////////////////////////

// ekosmas version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);
    SET_APPROXIMATE(bsf_result.distance);
    printf("Bsf Result %f\n",bsf_result.distance);
    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        NUM_PRIORITY_QUEUES = maxquerythread/2;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t*)*NUM_PRIORITY_QUEUES);
    int queuelabel[NUM_PRIORITY_QUEUES];                                            // EKOSMAS, 21 SEPTEMBER 2020: REMOVED

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER;                   // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
    pthread_barrier_t lock_barrier;
    pthread_barrier_init(&lock_barrier, NULL, maxquerythread);

    pthread_mutex_t ququelock[NUM_PRIORITY_QUEUES];
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size/NUM_PRIORITY_QUEUES, cmp_pri, get_pri, set_pri, get_pos, set_pos);
        pthread_mutex_init(&ququelock[i], NULL);
        queuelabel[i]=1;                                                 
    }

    volatile int node_counter = 0;                                              // EKOSMAS, 29 AUGUST 2020: added volatile

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].lock_bsf = &lock_bsf;                                     // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                                 // the BSF
        workerdata[i].lock_barrier = &lock_barrier;                             // barrier between queue inserts and queue pops
        workerdata[i].alllock = ququelock;                                      // all queues' locks
        workerdata[i].allqueuelabel = queuelabel;                               // ??? How is this used?         
        workerdata[i].allpq = allpq;                                            // priority queues
        workerdata[i].startqueuenumber = i%NUM_PRIORITY_QUEUES;                            // initial priority queue to start
        workerdata[i].workernumber = i;
    }
        
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas, (void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    pthread_barrier_destroy(&lock_barrier);

    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;

    // Free the nodes that where not popped.
}

///////////////////////////////////////////////////////////////////////////
int process_queue_node_vorgias(MESSI_workerdata_ekosmas *input_data, int i, int *checks,attribute_type*attribute)
{
    pthread_mutex_lock(&(input_data->alllock[i]));
    query_result *n = pqueue_pop(input_data->allpq[i]);
    pthread_mutex_unlock(&(input_data->alllock[i]));
    if(n == NULL) {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    isax_node_record * record =malloc(sizeof(isax_node_record));////////////////////////////////
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else {
        // If it is a leaf, check its real distance.
        if (((isax_node*)n->node)->is_leaf) {
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_vorgias(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance,attribute,&record);
            if (distance < bsfdisntance) {
                pthread_mutex_lock(input_data->lock_bsf);
                if(distance < bsf_result->distance)
                {
                    bsf_result->distance = distance;
                    bsf_result->node = n->node;
                    free(bsf_result->record);//////////////////////////////////
                    bsf_result->record = record;//////////////////////////////////
                }else{
                    free(record);
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }

        }
        
    }
    free(n);

    return 1;
}
//////////////////////////////////////////////////////////////////////////////////////

int process_queue_node_ekosmas(MESSI_workerdata_ekosmas *input_data, int i, int *checks)
{
    pthread_mutex_lock(&(input_data->alllock[i]));
    query_result *n = pqueue_pop(input_data->allpq[i]);
    pthread_mutex_unlock(&(input_data->alllock[i]));
    if(n == NULL) {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else {
        // If it is a leaf, check its real distance.
        if (((isax_node*)n->node)->is_leaf) {
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance);
            if (distance < bsfdisntance) {
                pthread_mutex_lock(input_data->lock_bsf);
                if(distance < bsf_result->distance)
                {
                    bsf_result->distance = distance;
                    bsf_result->node = n->node;
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }

        }
        
    }
    free(n);

    return 1;
}

/////////////////////////////////////////////////////////////////////////////////////////
void* exact_search_worker_inmemory_hybridpqueue_vorgias(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    int startqueuenumber = input_data->startqueuenumber;
    
    // A. Populate Queues
    //COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node=input_data->nodelist[current_root_node_number];
            if(evalSubtree(current_root_node,input_data->attribute,index)){////////////////////////////////////////
                //printf("inserting root node %d\n",current_root_node_number);
                insert_tree_node_m_hybridpqueue_vorgias(paa, current_root_node, index, bsfdisntance, input_data->allpq, input_data->alllock, &tnumber,input_data->attribute);            
            }
    }

    // Wait all threads to fill in queues
    pthread_barrier_wait(input_data->lock_barrier);

    // B. Processing my queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0;                                                                     // This is just for debugging. It can be removed!
    while (process_queue_node_vorgias(input_data, startqueuenumber, &checks,input_data->attribute)) {         // while a candidate queue node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    if( (input_data->allqueuelabel[startqueuenumber])==1)
    {
        (input_data->allqueuelabel[startqueuenumber])=0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while(n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // D. Process other uncompleted queues
    while(1)                                                                    // ??? EKOSMAS: Why is this while(1) required?
    {         
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++) {
            if((input_data->allqueuelabel[i]) == 1) {
                finished = false;
                while(process_queue_node_vorgias(input_data, i, &checks,input_data->attribute)) {
                    ;
                }
            }
        }

        if (finished) {
            break;
        }
    }   

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }
}
//////////////////////////////////////////////////////////////////////////////////////////////


// ekosmas version
void* exact_search_worker_inmemory_hybridpqueue_ekosmas(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas *input_data = (MESSI_workerdata_ekosmas*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    int startqueuenumber = input_data->startqueuenumber;
    
    // A. Populate Queues
    //COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node=input_data->nodelist[current_root_node_number];
            insert_tree_node_m_hybridpqueue_ekosmas(paa, current_root_node, index, bsfdisntance, input_data->allpq, input_data->alllock, &tnumber);            
    }

    // Wait all threads to fill in queues
    pthread_barrier_wait(input_data->lock_barrier);

    // B. Processing my queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0;                                                                     // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas(input_data, startqueuenumber, &checks)) {         // while a candidate queue node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    if( (input_data->allqueuelabel[startqueuenumber])==1)
    {
        (input_data->allqueuelabel[startqueuenumber])=0;
        pthread_mutex_lock(&(input_data->alllock[startqueuenumber]));
        query_result *n;
        while(n = pqueue_pop(input_data->allpq[startqueuenumber]))
        {
            free(n);
        }
        pthread_mutex_unlock(&(input_data->alllock[startqueuenumber]));
    }


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // D. Process other uncompleted queues
    while(1)                                                                    // ??? EKOSMAS: Why is this while(1) required?
    {         
        bool finished = true;
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++) {
            if((input_data->allqueuelabel[i]) == 1) {
                finished = false;
                while(process_queue_node_ekosmas(input_data, i, &checks)) {
                    ;
                }
            }
        }

        if (finished) {
            break;
        }
    }   

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }
}

///////////////////////////////////////////////////////////////////////////////
void insert_tree_node_m_hybridpqueue_vorgias(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber,attribute_type* attribute)
{   

    // printf ("executing insert_tree_node_m_hybridpqueue_ekosmas\n");

    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END


    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            //printf("inserting leaf %d\n",node->leaf_id);
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   

            if (((isax_node*)node->left_child)->isax_cardinalities != NULL && evalSubtree(node->left_child,attribute,index))/////////////////////////////                                                       // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                //printf("inserting left %d\n",*attribute);
                insert_tree_node_m_hybridpqueue_vorgias(paa,node->left_child,index, bsf,pq,lock_queue,tnumber,attribute);
            }
            if (((isax_node*)node->right_child)->isax_cardinalities != NULL && evalSubtree(node->right_child,attribute,index))/////////////////////////////                                                     // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                //printf("inserting right \n",*attribute);
                insert_tree_node_m_hybridpqueue_vorgias(paa,node->right_child,index,bsf,pq,lock_queue,tnumber,attribute);
            }
        }
    }
}
/////////////////////////////////////////////////////////////////////////////////////


// ekosmas version
void insert_tree_node_m_hybridpqueue_ekosmas(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t **pq, pthread_mutex_t *lock_queue, int *tnumber)
{   

    // printf ("executing insert_tree_node_m_hybridpqueue_ekosmas\n");

    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END


    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pthread_mutex_lock(&lock_queue[*tnumber]);
            pqueue_insert(pq[*tnumber], mindist_result);
            pthread_mutex_unlock(&lock_queue[*tnumber]);
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   
            if (((isax_node*)node->left_child)->isax_cardinalities != NULL)                                                       // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa,node->left_child,index, bsf,pq,lock_queue,tnumber);
            }
            if (((isax_node*)node->right_child)->isax_cardinalities != NULL)                                                      // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas(paa,node->right_child,index,bsf,pq,lock_queue,tnumber);
            }
        }
    }
}

// -------------------------------------
// -------------------------------------

// ekosmas-EP version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_EP(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas(ts, paa, index);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    pqueue_t **allpq = malloc(sizeof(pqueue_t*) * maxquerythread);

    pthread_t threadid[maxquerythread];
    MESSI_workerdata_ekosmas_EP workerdata[maxquerythread];
    pthread_mutex_t lock_bsf = PTHREAD_MUTEX_INITIALIZER;                   // EKOSMAS, 29 AUGUST 2020: Changed this to mutex
 
    for (int i = 0; i < maxquerythread; i++)
    {
        allpq[i] = pqueue_init(index->settings->root_nodes_size, cmp_pri, get_pri, set_pri, get_pos, set_pos);
    }

    volatile int node_counter = 0;                                              // EKOSMAS, 29 AUGUST 2020: added volatile

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in som global variable.
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].pq = allpq[i];                                                
        workerdata[i].lock_bsf = &lock_bsf;                                     // a lock to update BSF
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].bsf_result = &bsf_result;                                 // the BSF
        workerdata[i].workernumber = i;

    }
        
    for (int i = 0; i < maxquerythread; i++) {
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_EP, (void*)&(workerdata[i]));
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    // Free the priority queue.
    for (int i = 0; i < maxquerythread; i++)
    {
        pqueue_free(allpq[i]);
    }
    free(allpq);

    return bsf_result;

    // Free the nodes that where not popped.
}
int process_queue_node_ekosmas_EP(MESSI_workerdata_ekosmas_EP *input_data, pqueue_t *my_pq, int *checks)
{
    query_result *n = pqueue_pop(my_pq);
    if(n == NULL) {
        return 0;
    }

    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    if (n->distance > bsfdisntance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        return 0;
    }
    else {
        // If it is a leaf, check its real distance.
        if (((isax_node*)n->node)->is_leaf) {
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas(input_data->index, n->node, input_data->ts, input_data->paa, bsfdisntance);
            if (distance < bsfdisntance) {
                pthread_mutex_lock(input_data->lock_bsf);
                if(distance < bsf_result->distance)
                {
                    bsf_result->distance = distance;
                    bsf_result->node = n->node;
                }
                pthread_mutex_unlock(input_data->lock_bsf);
            }
        }
    }
    free(n);

    return 1;
}
// ekosmas-EP version
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_EP(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_EP*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_EP *input_data = (MESSI_workerdata_ekosmas_EP*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    pqueue_t *my_pq = input_data->pq;  
    
    query_result *bsf_result = input_data->bsf_result;
    float bsfdisntance = bsf_result->distance;
    
    // A. Populate Queues
    //COUNT_QUEUE_TIME_START
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            insert_tree_node_m_hybridpqueue_ekosmas_EP(paa, current_root_node, index, bsfdisntance, my_pq);            

    }

    // B. Processing my queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    int checks = 0;                                                                     // This is just for debugging. It can be removed!
    while (process_queue_node_ekosmas_EP(input_data, my_pq, &checks)) {         // while a candidate queueu node with smaller distance exists, compute actual distances
        ;
    }

    // C. Free any element left in my queue
    query_result *n;
    while(n = pqueue_pop(my_pq))
    {
        free(n);
    }   


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }
}
// ekosmas-EP version
void insert_tree_node_m_hybridpqueue_ekosmas_EP(float *paa, isax_node *node, isax_index *index, float bsf, pqueue_t *my_pq)
{   
    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END


    if(distance < bsf)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;
            pqueue_insert(my_pq, mindist_result);
        }
        else
        {   
            if (((isax_node*)node->left_child)->isax_cardinalities != NULL)                                                       // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas_EP(paa,node->left_child,index, bsf,my_pq);
            }
            if (((isax_node*)node->right_child)->isax_cardinalities != NULL)                                                      // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                insert_tree_node_m_hybridpqueue_ekosmas_EP(paa,node->right_child,index,bsf,my_pq);
            }
        }
    }
}

// -------------------------------------
// -------------------------------------

// ekosmas-lf version
query_result exact_search_ParISnew_inmemory_hybrid_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id) 
{   
    printf("Third phase = %d\n",third_phase);
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas_lf(ts, paa, index, parallelism_in_subtree);
    query_result * volatile bsf_result_p = &bsf_result;
    SET_APPROXIMATE(bsf_result.distance);
    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }
    if (maxquerythread == 1) {
        NUM_PRIORITY_QUEUES = 1;
    }
    else {
        switch (third_phase) {
            case 1: 
            case 2: 
            case 3: 
            case 5:
            case 10: 
            case 12: NUM_PRIORITY_QUEUES = maxquerythread;
            break;  
            case 4: 
            case 6:
            case 11: 
            case 13: NUM_PRIORITY_QUEUES = maxquerythread/2;
            break;  
            case 7:  
            case 8: NUM_PRIORITY_QUEUES = 1;
            break;  
        }
    }    

    // pqueue_t **allpq = malloc(sizeof(pqueue_t*) * maxquerythread);
    // for (int i = 0; i < maxquerythread; i++)
    // {
    //     allpq[i] = pqueue_init(index->settings->root_nodes_size, cmp_pri, get_pri, set_pri, get_pos, set_pos);
    // }
    pqueue_t **allpq = calloc(NUM_PRIORITY_QUEUES, sizeof(pqueue_t*));
    query_result ***allpq_data = malloc(sizeof(query_result **) * NUM_PRIORITY_QUEUES);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq_data[i] = calloc (index->settings->root_nodes_size, sizeof(query_result *));
    }
    volatile unsigned long *next_queue_data_pos = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));
    volatile unsigned char *queue_finished = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile unsigned char *helper_queue_exist = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile int *fai_queue_counters = calloc(NUM_PRIORITY_QUEUES, sizeof(int));
    volatile float **queue_bsf_distance = calloc(NUM_PRIORITY_QUEUES, sizeof(float *));

    volatile int node_counter = 0;                                                      // EKOSMAS, 29 AUGUST 2020: added volatile
    volatile int *sorted_array_counter = calloc(NUM_PRIORITY_QUEUES, sizeof(int));      // EKOSMAS, 02 OCTOBER 2020: added, 14 OCTOBER 2020: changed

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in some global variable.
    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];


    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
        workerdata[i].allpq = allpq;
        workerdata[i].allpq_data = allpq_data;
        workerdata[i].queue_finished = queue_finished;
        workerdata[i].fai_queue_counters = fai_queue_counters;
        workerdata[i].queue_bsf_distance = queue_bsf_distance;
        workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
        workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
        workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
    }

    pthread_t threadid[maxquerythread];
    for (int i = 0; i < maxquerythread; i++) {
        switch (third_phase) {
            case 1: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed, (void*)&(workerdata[i])); 
            break;  
            case 2: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper, (void*)&(workerdata[i])); 
            break; 
            case 3: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping, (void*)&(workerdata[i])); 
            break;  
            case 4: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping, (void*)&(workerdata[i])); 
            break;  
            case 5: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping, (void*)&(workerdata[i])); 
            break;  
            case 6: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping, (void*)&(workerdata[i])); 
            break;  
            case 7: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help, (void*)&(workerdata[i])); 
            break;   
            case 8: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array, (void*)&(workerdata[i])); 
            break;  
            case 10: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper, (void*)&(workerdata[i])); 
            break;  
            case 11: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper, (void*)&(workerdata[i])); 
            break;  
            case 12: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping, (void*)&(workerdata[i])); 
            break;  
            case 13: pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping, (void*)&(workerdata[i])); 
            break;            
        }
        
    }
    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }
/*
    for(int k = 0 ; k < nodelist->node_amount; k++){  //reset fai counters
             ((isax_node*)nodelist->nlist[k])->node_counter = 1;

     }
*/
    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        if (!allpq[i]) {
            continue;
        }

        pqueue_free(allpq[i]);
    }
    free(allpq);
    free((void *)queue_finished);
    free((void *)helper_queue_exist);

    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}





//geopat single receive buffer. Tree only for BSF. Queues from receive buffer.
query_result exact_search_ParISnew_inmemory_geopat(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id,unsigned long dataset_size) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas_lf_geopat(ts, paa, index, parallelism_in_subtree);
    query_result * volatile bsf_result_p = &bsf_result;
    printf("Bsf Result distance= %f\n",bsf_result.distance);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

    static pq_t * pq_main ;
    _init_gc_subsystem();
    pq_main = pq_init(1000);
    unsigned long queue_insertinos = 0;


  volatile int node_counter = 0; 



    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in some global variable.
    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    volatile unsigned char *already_inserted = malloc(sizeof(unsigned char) * dataset_size);
    for(unsigned long i = 0 ; i < dataset_size;i++){
        already_inserted[i] = 0 ;
    }

    
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
       // workerdata[i].allpq = allpq;
       // workerdata[i].allpq_data = allpq_data;
        workerdata[i].main_queue = pq_main;
       // workerdata[i].queue_finished = queue_finished;                          //Added Global pointer to main queue
       // workerdata[i].fai_queue_counters = fai_queue_counters;
       // workerdata[i].queue_bsf_distance = queue_bsf_distance;
       // workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
       // workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
       // workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
        workerdata[i].queue_insertions = &queue_insertinos;

    }

    pthread_t threadid[maxquerythread];
   // printf("Starting Threads\n");
    for (int i = 0; i < maxquerythread; i++) { 
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_single_priority_queue_geopat, (void*)&(workerdata[i])); 
    }





    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);

    for(int k = 0 ; k < number_of_subtrees; k++){  //reset fai counters
        if(current_fbl_node->node[k] != NULL){
            current_fbl_node->node[k]->node_counter = 1;
        }
     }

  /*
    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        if (!allpq[i]) {
            continue;
        }

        pqueue_free(allpq[i]);
    }
    free(allpq);
    free((void *)queue_finished);
    free((void *)helper_queue_exist);
*/
    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}

//geopat single receive buffer. Tree only for BSF. Queues from receive buffer.
query_result exact_search_ParISnew_inmemory_geopat_multiple_queues(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id,unsigned long dataset_size) 
{   
    query_result bsf_result = approximate_search_inmemory_pRecBuf_ekosmas_lf_geopat(ts, paa, index, parallelism_in_subtree);
    query_result * volatile bsf_result_p = &bsf_result;
    printf("Bsf Result distance= %f\n",bsf_result.distance);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

      pq_t **pq_main = malloc(sizeof(pq_t*) * 40);
      

     for(int i = 0 ; i <40; i++){ 
        _init_gc_subsystem();
        pq_main[i] = pq_init(1000);
     }

   
    
    //pq_main = pq_init(1000);
    unsigned long queue_insertinos = 0;


   // struct IsaxPQ *main_queue = initializeQueue(index->fbl->initial_buffer_size,main_queue);;
    //struct IsaxPQ **main_q = malloc(sizeof(struct IsaxPQ *));
    //main_q = &main_queue;
  volatile int node_counter = 0; 
//-----------------------------------Not Used-----------------------------------------------------------
   /*
    volatile unsigned long *next_queue_data_pos = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned long));
    volatile unsigned char *queue_finished = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile unsigned char *helper_queue_exist = calloc(NUM_PRIORITY_QUEUES, sizeof(unsigned char));
    volatile int *fai_queue_counters = calloc(NUM_PRIORITY_QUEUES, sizeof(int));
    volatile float **queue_bsf_distance = calloc(NUM_PRIORITY_QUEUES, sizeof(float *));

    
    unsigned long node_counter_queue = 0;                                                     // EKOSMAS, 29 AUGUST 2020: added volatile
    volatile int *sorted_array_counter = calloc(NUM_PRIORITY_QUEUES, sizeof(int));      // EKOSMAS, 02 OCTOBER 2020: added, 14 OCTOBER 2020: changed


    pqueue_t **allpq = calloc(NUM_PRIORITY_QUEUES, sizeof(pqueue_t*));
    query_result ***allpq_data = malloc(sizeof(query_result **) * NUM_PRIORITY_QUEUES);
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        allpq_data[i] = calloc (index->settings->root_nodes_size, sizeof(query_result *));
    }
    */
//-------------------------------------------------------------------------------------------------------  


    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in some global variable.
    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];
    
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
       // workerdata[i].allpq = allpq;
       // workerdata[i].allpq_data = allpq_data;
        //workerdata[i].main_queue = pq_main;
        workerdata[i].main_queues = pq_main;
       // workerdata[i].queue_finished = queue_finished;                          //Added Global pointer to main queue
       // workerdata[i].fai_queue_counters = fai_queue_counters;
       // workerdata[i].queue_bsf_distance = queue_bsf_distance;
       // workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
       // workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
       // workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
        workerdata[i].queue_insertions = &queue_insertinos;

    }

    pthread_t threadid[maxquerythread];
   // printf("Starting Threads\n");
    for (int i = 0; i < maxquerythread; i++) { 
        pthread_create(&(threadid[i]), NULL, exact_search_worker_inmemory_40_priority_queue_geopat, (void*)&(workerdata[i])); 
    }

     parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
     parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
     int number_of_subtrees = (int)pow(2, index->settings->paa_segments);

     for(int k = 0 ; k < number_of_subtrees; k++){  //reset fai counters
        if(current_fbl_node->node[k] != NULL){
            current_fbl_node->node[k]->node_counter = 1;
        }
     }


    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }
  /*
    // Free the priority queue.
    for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    {
        if (!allpq[i]) {
            continue;
        }

        pqueue_free(allpq[i]);
    }
    free(allpq);
    free((void *)queue_finished);
    free((void *)helper_queue_exist);
*/
    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}

query_result find_distances(ts_type *ts, ts_type *paa, isax_index *index, node_list *nodelist,
                           float minimum_distance, const char parallelism_in_subtree, const int third_phase, const int query_id,unsigned long timeSeriesNum) 
{   
    query_result bsf_result;
    bsf_result.node = NULL;
    bsf_result.distance = FLT_MAX;
    query_result * volatile bsf_result_p = &bsf_result;
    printf("Bsf Result distance= %f\n",bsf_result.distance);
    SET_APPROXIMATE(bsf_result.distance);

    // Early termination...
    if (bsf_result.distance == 0) {
        return bsf_result;
    }

  volatile int node_counter = 0; 
  float *distance_array = malloc(sizeof(float) * timeSeriesNum);
  unsigned long k = 0;
    for( k = 0 ; k < timeSeriesNum;k++){
        distance_array[k] = 0 ;
    }
    int finished_buffer = 0;

    // EKOSMAS: change this so that only i is passed as an argument and all the others are only once in some global variable.
    MESSI_workerdata_ekosmas_lf workerdata[maxquerythread];
    
    for (int i = 0; i < maxquerythread; i++)
    {
        workerdata[i].ts = ts;                                                  // query ts
        workerdata[i].paa = paa;                                                // query paa
       // workerdata[i].allpq = allpq;
       // workerdata[i].allpq_data = allpq_data;
        //workerdata[i].main_queue = pq_main;
       // workerdata[i].queue_finished = queue_finished;                          //Added Global pointer to main queue
       // workerdata[i].fai_queue_counters = fai_queue_counters;
       // workerdata[i].queue_bsf_distance = queue_bsf_distance;
       // workerdata[i].helper_queue_exist = helper_queue_exist;
        workerdata[i].nodelist = nodelist->nlist;                               // the list of subtrees
        workerdata[i].amountnode = nodelist->node_amount;                       // the total number of (non empty) subtrees
        workerdata[i].index = index;
        workerdata[i].minimum_distance = minimum_distance;                      // its value is FTL_MAX
        workerdata[i].node_counter = &node_counter;                             // a counter to perform FAI and acquire subtrees
       // workerdata[i].sorted_array_counter = sorted_array_counter;
        workerdata[i].bsf_result_p = &bsf_result_p;                             // the BSF
        workerdata[i].workernumber = i;
        workerdata[i].parallelism_in_subtree = parallelism_in_subtree;
       // workerdata[i].next_queue_data_pos = next_queue_data_pos;
        workerdata[i].query_id = query_id;
        //workerdata[i].queue_insertions = &queue_insertinos;
        workerdata[i].timeSeriesNum = timeSeriesNum;
        workerdata[i].distances_array = distance_array;
        workerdata[i].finished_buffer = &finished_buffer;
    }

    pthread_t threadid[maxquerythread];
   // printf("Starting Threads\n");
    for (int i = 0; i < maxquerythread; i++) { 
        pthread_create(&(threadid[i]), NULL, calculate_eyclidian_distances, (void*)&(workerdata[i])); 
    }

    


    for (int i = 0; i < maxquerythread; i++) {
        pthread_join(threadid[i], NULL);
    }

    float min = FLT_MAX;
    unsigned long m = 0 ;
    for(m=0;m<timeSeriesNum;m++){
        if(distance_array[m]<= min ){
            min = distance_array[m];
        }
    }

    bsf_result_p->distance = min;

    return *bsf_result_p;

    // +++ Free the nodes that where not popped.
}

static int help_queue_node(int node_id, MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, size_t pq_size, int *checks, const char parallelism_in_subtree, const unsigned char single_node, int move_num_pos, float queue_bsf)
{

    // printf ("Thread [%d]: help_queue_node: Helping queue [%d] with single_node = [%d]\n", input_data->workernumber, pq_id, single_node);fflush(stdout);

    // if (node_id < 1) {
    //     printf ("$$$ ----> MAN node_id is less than 1 !!! specifically [%d] <---- $$$\n", node_id);
    // }

    if (single_node) {                  // move inside priority queue to find the appropriate node in the path defined by queue_bsf, which is move_num_pos positions after node_id
        for (; move_num_pos > 0; move_num_pos--) {
            while (1) {
                if (left(node_id) < pq_size && ((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                    node_id = left(node_id);
                    break;
                }
                
                if (right(node_id) < pq_size && ((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                    node_id = right(node_id);
                    break;
                }

                // while (node_id > 1 && right(parent(node_id)) < pq_size && node_id == right(parent(node_id))) {
                while (node_id > 1 && (right(parent(node_id)) >= pq_size || node_id == right(parent(node_id))) ) {
                    node_id = parent(node_id);
                }
                
                // if (node_id != right(parent(node_id))) {
                if (node_id > 1 && right(parent(node_id)) < pq_size) {
                    node_id = right(parent(node_id));
                    if (((query_result *)pq->d[node_id])->distance <= queue_bsf) {
                        break;
                    }
                }

                return -1;
            }
        }        
    }

    // printf ("Thread [%d]: help_queue_node: Helping queue [%d] with node_id = [%d]\n", input_data->workernumber, pq_id, node_id);fflush(stdout);

    if (node_id >= pq_size || input_data->queue_finished[pq_id]) {
        return -1;
    }

    query_result *n = pq->d[node_id];
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;

    // if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
    if (n->distance > bsfdistance) {                                                        // The best node has a worse mindist, so search is finished! // EKOSMAS: Changed on 17 September 2020
        return node_id;
    }

    // If it is a leaf, check its real distance.
    if (((isax_node*)n->node)->is_leaf) {                                                                 // EKOSMAS 15 SEPTEMBER 2020: This will always be a leaf!!!
        if (n->distance >= 0) {                       
            (*checks)++;                                                                    // This is just for debugging. It can be removed!

            float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

            // update bsf, if needed
            while (distance < bsfdistance) {

                // if (!single_node) {
                //     // printf("node with ID [%d] has not been checked previously... and it has better bsf!!!\n", node_id);
                //     fprintf(stderr, "node with ID [%d] has not been checked previously... and it has better bsf!!!\n", node_id);
                //     if (node_id == 5) {
                //         // printf ("\n--------------\n");
                //         fprintf(stderr, "\n--------------\n");
                //         for (int iii = 1; iii < 16; iii++) {
                //             // printf ("Node [%d] has distance [%f] \n\n", iii, ((query_result *)pq->d[iii])->distance);
                //             fprintf (stderr, "Node [%d] has distance [%f] \n\n", iii, ((query_result *)pq->d[iii])->distance);
                //         }
                //         // printf ("--------------\n");
                //         fprintf(stderr, "--------------\n");
                //     }
                // }    

                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->node = n->node;

                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }

                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }
            
            // float tmp_distance = n->distance;
            // if (tmp_distance >= 0) {
            //     n->distance = tmp_distance * -1;           //  in case of slow path, inform that this queue node has been processed
            // }

            if (n->distance >= 0) {                     // EKOSMAS: CHANGED 25 SEPTEMBER 2020 
                n->distance = -1;                       //  in case of slow path, inform that this queue node has been processed
            }            
        }
        else {
            // printf("Skipping actual dinstance calculation since node is already calculated. This is good!!!\n");
        }
    }
    else {
        printf ("This queue node is not a leaf. WTF???\n"); fflush(stdout);
    }

    if (!single_node) {
        help_queue_node(left(node_id), input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
        help_queue_node(right(node_id), input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
    }
    else {
        return node_id;
    }

    // free(n);     // EKOSMAS 15 SEPTEMBER 2020: This is dangerous!
}

static inline void help_queue(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{

    // printf ("help_queue - START\n"); fflush(stdout);

    size_t pq_size = pq->size;

    // if (input_data->workernumber == 0) {
    //     printf ("pq_size = [%8d]\t", pq_size); fflush(stdout);
    // }

    if (pq_size != 0) {

        // initialize this queue's currently known bsf distance. This will define a single "path of interest" inside this queue.
        if (input_data->queue_bsf_distance[pq_id] == NULL) {
            float *tmp_float = malloc(sizeof(float));
            *tmp_float = (*(input_data->bsf_result_p))->distance;
            if (!CASPTR(&(input_data->queue_bsf_distance[pq_id]), NULL, tmp_float)) {
                free(tmp_float);
            }
        }

        float queue_bsf = *(input_data->queue_bsf_distance[pq_id]);

        // printf ("help_queue - queue_bsf = [%f] \t ", queue_bsf); fflush(stdout);

        int queue_node_path_pos = 0;
        int prev_queue_node_path_pos = 1;
        int start_queue_node_path_pos = 1;
        int move_num_pos;
        while (start_queue_node_path_pos > 0) {
            queue_node_path_pos = __sync_fetch_and_add(&(input_data->fai_queue_counters[pq_id]), 1) + 1;
            move_num_pos = queue_node_path_pos - prev_queue_node_path_pos;
            prev_queue_node_path_pos = queue_node_path_pos;

            // printf ("Thread [%d]: help_queue: Helping queue [%d] with start_queue_node_path_pos=[%d], queue_node_path_pos=[%d], move_num_pos=[%d]\n", input_data->workernumber, pq_id, start_queue_node_path_pos, queue_node_path_pos, move_num_pos);fflush(stdout);
            start_queue_node_path_pos = help_queue_node(start_queue_node_path_pos, input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 1, move_num_pos, queue_bsf);
        }

        // Search of unprocessed queue nodes in the "path of interest"
        help_queue_node(1, input_data, pq, pq_id, pq_size, checks, input_data->parallelism_in_subtree, 0, 0, 0);
    }

    if (!input_data->queue_finished[pq_id]) {
        input_data->queue_finished[pq_id] = 1;
    }

    // printf ("help_queue - END\n"); fflush(stdout);
}

int process_queue_node_ekosmas_lf(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{
    query_result *n = pqueue_pop_lf(pq, pq->size);
    if(n == NULL) {
        if (!input_data->queue_finished[pq_id]) {
            input_data->queue_finished[pq_id] = 1;
        }
        return 0;
    }

    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;
    if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        if (!input_data->queue_finished[pq_id]) {
            input_data->queue_finished[pq_id] = 1;
        }
        return 0;
    }

    // If it is a leaf, check its real distance.
    if (((isax_node*)n->node)->is_leaf) {                                         // EKOSMAS 15 SEPTEMBER 2020: This will always be a leaf!!!
        (*checks)++;                                                                    // This is just for debugging. It can be removed!

        float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

        // update bsf, if needed
        while (distance < bsfdistance) {
            query_result *bsf_result_new = malloc(sizeof(query_result));
            bsf_result_new->distance = distance;
            bsf_result_new->node = n->node;

            if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                free(bsf_result_new);
            }

            bsf_result = *(input_data->bsf_result_p);
            bsfdistance = bsf_result->distance;
        }

        if (n->distance >= 0) {
            n->distance = -1;           //  in case of slow path, inform that this queue node has been processed
        }
    }
    else {
        printf ("This queue node is not a leaf. WTF???\n"); fflush(stdout);
    }

    // free(n);     // EKOSMAS 15 SEPTEMBER 2020: This is dangerous!

    return 1;
}
static inline void create_heap_from_data_queue(int pq_id, query_result ***allpq_data, volatile unsigned long *next_queue_data_pos, pqueue_t **allpq) {

    pqueue_t *local_pq = pqueue_init(next_queue_data_pos[pq_id], cmp_pri, get_pri, set_pri, get_pos, set_pos);

    // for (int i = 0 ; i<next_queue_data_pos[pq_id] && allpq_data[pq_id][i] != NULL && !allpq[pq_id]; i++) {       // EKOSMAS: CHANGED 25 SEPTEMBER 2020
    for (int i = 0 ; i<next_queue_data_pos[pq_id] && !allpq[pq_id]; i++) {
        if (!allpq_data[pq_id][i]) {
            continue;
        }
        pqueue_insert(local_pq, allpq_data[pq_id][i]);
    }

    if (!allpq[pq_id] && !CASPTR(&allpq[pq_id], NULL, local_pq)) {
        pqueue_free(local_pq);
    }
}
int compare_sorted_array_items(const void *a, const void *b) {
    float dist_a = (*((query_result**)a))->distance;
    float dist_b = (*((query_result**)b))->distance;

    if (dist_a < dist_b) return -1;
    if (dist_a > dist_b) return 1;
    return 0;
}
static inline void create_sorted_array_from_data_queue(int pq_id, query_result ***allpq_data, volatile unsigned long *next_queue_data_pos, pqueue_t **allpq) {

    pqueue_t *local_pq = pqueue_init(next_queue_data_pos[pq_id], cmp_pri, get_pri, set_pri, get_pos, set_pos);

    // query_result *local_array = 

    // for (int i = 0 ; i<next_queue_data_pos[pq_id] && allpq_data[pq_id][i] != NULL && !allpq[pq_id]; i++) {       // EKOSMAS: CHANGED 25 SEPTEMBER 2020
    size_t j = 0;
    for (int i = 0; i<next_queue_data_pos[pq_id] && !allpq[pq_id]; i++) {
        if (!allpq_data[pq_id][i]) {
            continue;
        }
        // pqueue_insert(local_pq, allpq_data[pq_id][i]);
        local_pq->d[j] = (void *)allpq_data[pq_id][i];
        j++;
    }

    local_pq->size = j;
    // printf("sorted array has size: [%d] and next_queue_data_pos[pq_id] is: [%d]\n", j, next_queue_data_pos[pq_id]);

    // printf("Printing unsorted array: \n");
    // for (int i=0; i<30; i++) {
    //     printf("A[%2d] = [%12.5f] \t", i, ((query_result *)local_pq->d[i])->distance);
    // }    
    // printf("\n");


    // qsort(void *base, size_t nitems, size_t size, int (*compar)(const void *, const void*))
    qsort(local_pq->d, j, sizeof(query_result *), compare_sorted_array_items);

    // printf("Printing sorted array: \n");
    // for (int i=0; i<30; i++) {
    //     printf("A[%2d] = [%12.5f] \t", i, ((query_result *)local_pq->d[i])->distance);
    // }    
    // printf("\n");

    // EKOSMAS: 18/01/2022 - pqueue_free also when CAS in never executed (i.e. it holds that allpq[pq_id]!=NULL).
    if (!allpq[pq_id] && !CASPTR(&allpq[pq_id], NULL, local_pq)) {
        pqueue_free(local_pq);
    }
}
int process_sorted_array_element_ekosmas_lf(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{

    int element_id = input_data->sorted_array_counter[pq_id];
    input_data->sorted_array_counter[pq_id] = element_id + 1;

    size_t pq_size = pq->size;
    if (element_id >= pq_size) {
        return 0;
    }

    query_result *n = (query_result *) pq->d[element_id];

    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdistance = bsf_result->distance;
    if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
        if (!input_data->queue_finished[pq_id]) {
            input_data->queue_finished[pq_id] = 1;
        }
        return 0;
    }

    // If it is a leaf, check its real distance.
    if (((isax_node*)n->node)->is_leaf) {                                         // EKOSMAS 15 SEPTEMBER 2020: This will always be a leaf!!!
        (*checks)++;                                                                    // This is just for debugging. It can be removed!

        float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

        // update bsf, if needed
        while (distance < bsfdistance) {
            query_result *bsf_result_new = malloc(sizeof(query_result));
            bsf_result_new->distance = distance;
            bsf_result_new->node = n->node;

            if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                free(bsf_result_new);
            } 
            // else {
            //     printf ("process_sorted_array_element_ekosmas_lf: updated BSF from [%f] to [%f]\n", bsf_result->distance, bsf_result_new->distance); fflush(stdout);
            // }

            bsf_result = *(input_data->bsf_result_p);
            bsfdistance = bsf_result->distance;
        }

        if (n->distance >= 0) {
            n->distance = -1;           //  in case of slow path, inform that this sorted array element has been processed
        }
    }
    else {
        printf ("This queue node is not a leaf. WTF???\n"); fflush(stdout);
    }

    // free(n);     // EKOSMAS 15 SEPTEMBER 2020: This is dangerous!

    return 1;
}
static inline void help_sorted_array(MESSI_workerdata_ekosmas_lf *input_data, pqueue_t *pq, int pq_id, int *checks, const char parallelism_in_subtree)
{

    // printf ("help_sorted_array - START\n"); fflush(stdout);

    size_t pq_size = pq->size;

    // if (input_data->workernumber == 0) {
    //     printf ("pq_size = [%8d]\t", pq_size); fflush(stdout);
    // }

    if (pq_size != 0) {

        // repeatedly take an element with FAI
        int element_id; 
        while ((element_id = __sync_fetch_and_add(&(input_data->sorted_array_counter[pq_id]), 1)) < pq_size) {
            // process it!
            query_result *n = (query_result *) pq->d[element_id];

            // printf ("help_sorted_array - processing [%d] with distance [%f]\n", element_id, n->distance); fflush(stdout);

            volatile query_result *bsf_result = *(input_data->bsf_result_p);
            float bsfdistance = bsf_result->distance;
            if (n->distance > bsfdistance || n->distance > input_data->minimum_distance) {         // The best node has a worse mindist, so search is finished!
                // printf ("help_sorted_array - found n with larger distance than bsf --> stop\n\n"); fflush(stdout);
                break;
            }

            // If it is a leaf, check its real distance.
            if (((isax_node*)n->node)->is_leaf) {                                         // EKOSMAS 15 SEPTEMBER 2020: This will always be a leaf!!!
                if (n->distance >= 0) {                                     // EKOSMAS 14 OCTOBER 2020: ADDED - I am not sure if distance should be used in case of sorted arrays! It may give no better results, and worse it may give worse results...
                    (*checks)++;                                                                    // This is just for debugging. It can be removed!

                    float distance = calculate_node_distance2_inmemory_ekosmas_lf(input_data->index, n, input_data->ts, input_data->paa, bsfdistance, parallelism_in_subtree);

                    // printf ("help_sorted_array - processing [%d], which has distance [%f] and bsf is [%f]\n", element_id, distance, bsfdistance); fflush(stdout);

                    // update bsf, if needed
                    while (distance < bsfdistance) {
                        query_result *bsf_result_new = malloc(sizeof(query_result));
                        bsf_result_new->distance = distance;
                        bsf_result_new->node = n->node;

                        if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                            free(bsf_result_new);
                        }
                        // else {
                        //     printf ("help_sorted_array: updated BSF from [%f] to [%f]\n", bsf_result->distance, bsf_result_new->distance); fflush(stdout);
                        // }

                        bsf_result = *(input_data->bsf_result_p);
                        bsfdistance = bsf_result->distance;
                    }

                    if (n->distance >= 0) {                     
                        n->distance = -1;                       //  in case of slow path, inform that this sorted array element has been processed
                    }     
                }
            }
            else {
                printf ("This queue node is not a leaf. WTF???\n"); fflush(stdout);
            }
        }

        // search all previous array elements and help any unprocessed

    }
    // else {
    //     printf ("help_sorted_array - queue empty!!\n"); fflush(stdout);
    // }

    if (!input_data->queue_finished[pq_id]) {
        input_data->queue_finished[pq_id] = 1;
    }

    // printf ("help_sorted_array - END\n"); fflush(stdout);
}

// ekosmas-lf version - NOT MIXED - Version with 80 queues
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_not_mixed(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber] && input_data->next_queue_data_pos[input_data->workernumber]) {
        create_heap_from_data_queue(input_data->workernumber, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }


    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check whether first processing my queue and then helping other heaps to be crated will result in better performance!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < maxquerythread; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // -------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check the code below, since a segmetation fault is thrown for the 10GB Dataset!!!
    // -------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // B. Processing my queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my queue has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree);
            // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < maxquerythread; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            // printf ("[OTHERS] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
            // printf ("[OTHERS] executing help_queue - enddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   

    // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: END\n");fflush(stdout);
}

// ekosmas-lf version - MIXED - Version with 80 queues and helping during queue processing, only after a helper exists.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_only_after_helper(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exists, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my queue has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
            // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // -------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check the code below, since a segmetation fault is thrown for the 10GB Dataset!!!
    // -------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            // printf ("[OTHERS] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
            // printf ("[OTHERS] executing help_queue - enddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   
}

// ekosmas-lf version - MIXED - Version with 40 queues and helping during queue processing from the beggining.
// ekosmas-lf version - MIXED - Version with 80 queues and helping during queue processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_full_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    // if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }


    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check whether first processing my queue and then helping other heaps to be crated will result in better performance!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    // B. Processing my queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        // int ret_val;
        // while ( !input_data->helper_queue_exist[input_data->workernumber] &&
        //         (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
        //     ;
        // }

        // if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
        //     input_data->queue_finished[input_data->workernumber] = 1;
        // }

        // // B.2. if helpers exist and my queue has not yet finished, execute slow path
        // if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {

        // B.2. if my queue has not yet finished, execute slow path
        if (!input_data->queue_finished[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
            // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
            // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_heap_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // -------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check the code below, since a segmetation fault is thrown for the 10GB Dataset!!!
    // -------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // B.3. Help non-finished queues
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            // if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
            //     input_data->helper_queue_exist[i] = 1;
            // }

            // printf ("[OTHERS] executing help_queue - starttt\n"); fflush(stdout);
            help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
            // printf ("[OTHERS] executing help_queue - enddd\n"); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   

    // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: END\n");fflush(stdout);
}

// ekosmas-lf version - MIXED - Version with 40 queues and no helping during queue processing.
// ekosmas-lf version - MIXED - Version with 80 queues and no helping during queue processing
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_no_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    // if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_heap_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }


    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check whether first processing my queue and then helping other heaps to be crated will result in better performance!!!
    // --------------------------------------------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


    // B. Processing my queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exist, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    if (input_data->workernumber >= NUM_PRIORITY_QUEUES) {
        return NULL;
    }
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber];
    if (my_pq) {
        int ret_val;
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_queue_node_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // // // B.2. if helpers exist and my queue has not yet finished, execute slow path
        // // if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {

        // // B.2. if my queue has not yet finished, execute slow path
        // if (!input_data->queue_finished[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        //     // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
        //     help_queue(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
        //     // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        // }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other priority queues to be created
    // if (!DO_NOT_HELP) {
    //     for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    //     {
    //         if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
    //             create_heap_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    //         }
    //     }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // -------------------------------------------------------------------------------------------------------------------
    // EKOSMAS 23 SEPTEMBER 2020 - TODO: Check the code below, since a segmetation fault is thrown for the 10GB Dataset!!!
    // -------------------------------------------------------------------------------------------------------------------
    // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    // B.3. Help non-finished queues
    // if (!DO_NOT_HELP) {
    //     for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
    //     {
    //         if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
    //             continue;
    //         }

    //         // if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
    //         //     input_data->helper_queue_exist[i] = 1;
    //         // }

    //         // printf ("[OTHERS] executing help_queue - starttt\n"); fflush(stdout);
    //         help_queue(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
    //         // printf ("[OTHERS] executing help_queue - enddd\n"); fflush(stdout);
    //     }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   

    // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: END\n");fflush(stdout);
}

// ekosmas-lf version - Version with 1 queue and helping during queue processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_queue_full_help(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    // if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my priority queue (Heap)
    if (!input_data->allpq[0] && input_data->next_queue_data_pos[0]) {
        create_heap_from_data_queue(0, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }

    // printf ("data_queue %d has size [%d] --- heap has size [%d] and it is valid [%d] \n", 0, input_data->next_queue_data_pos[0], input_data->allpq[0]->size, pqueue_is_valid(input_data->allpq[0])); fflush(stdout);

    // B. Processing priority queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. if priority queue has not yet finished, process it
    pqueue_t *my_pq = input_data->allpq[0];
    if (!input_data->queue_finished[0]) {
        help_queue(input_data, my_pq, 0, &checks, input_data->parallelism_in_subtree);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   

    // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: END\n");fflush(stdout);
}

// ekosmas-lf version - Version with 1 sorted array and helping during sorted array processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_single_sorted_array(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
    
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand()% NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    // if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    // }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // A.2. Create my sorted array
    if (!input_data->allpq[0] && input_data->next_queue_data_pos[0]) {
        create_sorted_array_from_data_queue(0, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }

    // printf ("data_queue %d has size [%d] --- heap has size [%d] and it is valid [%d] \n", 0, input_data->next_queue_data_pos[0], input_data->allpq[0]->size, pqueue_is_valid(input_data->allpq[0])); fflush(stdout);

    // B. Processing priority queue
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. if priority queue has not yet finished, process it
    pqueue_t *my_pq = input_data->allpq[0];
    if (!input_data->queue_finished[0]) {
        // help_queue(input_data, my_pq, 0, &checks, input_data->parallelism_in_subtree);
        help_sorted_array(input_data, my_pq, 0, &checks, input_data->parallelism_in_subtree);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   

    // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: END\n");fflush(stdout);
}
int process_queue_node_geopat(MESSI_workerdata_ekosmas_lf *input_data)
{
    
    //struct IsaxPQ *queue = input_data->main_queue[0];
    pq_t * queue = input_data->main_queue;
    int count = 0;
    unsigned long j = 0;
    while(1){

        query_result *curr_result = (query_result *)deletemin(queue);
        if(curr_result == NULL){
            break;
        }
        volatile query_result *bsf_result = *(input_data->bsf_result_p);
        float bsfdistance = bsf_result->distance;
        
        float distance = calculate_node_distance2_inmemory_geopat2(input_data->index, curr_result, input_data->ts, input_data->paa, bsfdistance,input_data->parallelism_in_subtree);
            while (distance < bsfdistance) {
                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->record = curr_result->record;
                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }
                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }
       // }
    }

    return 1;
}   

int process_queue_node_geopat2(MESSI_workerdata_ekosmas_lf *input_data)
{
    
    //struct IsaxPQ *queue = input_data->main_queue[0];
   
    int count = 0;
    unsigned long j = 0;
    int current_queue = 0;
    while(1){
        int found_queue = 0;
        if(count >0 && ((input_data->workernumber + count ) % 40 == input_data->workernumber)){
            break;
        }
        pq_t * queue = input_data->main_queues[(input_data->workernumber + count ) % 40 ];
        query_result *curr_result = (query_result *)deletemin(queue);
        if(curr_result == NULL){
            count++;
            continue;
        }
        volatile query_result *bsf_result = *(input_data->bsf_result_p);
        float bsfdistance = bsf_result->distance;
        isax_node *node = curr_result->node;
        
        float distance = calculate_node_distance2_inmemory_geopat2(input_data->index, curr_result, input_data->ts, input_data->paa, bsfdistance,input_data->parallelism_in_subtree);
            while (distance < bsfdistance) {
                query_result *bsf_result_new = malloc(sizeof(query_result));
                bsf_result_new->distance = distance;
                bsf_result_new->record = curr_result->record;
                if (!CASPTR(input_data->bsf_result_p, bsf_result, bsf_result_new)) {
                    free(bsf_result_new);
                }
                bsf_result = *(input_data->bsf_result_p);
                bsfdistance = bsf_result->distance;
            }
       // }
    }

    return 1;
}

//----------------------- Mergesort----------
//MergeSort Geek
inline void merge(float arr[], int l, int m, int r)
{
    int i, j, k;
    int n1 = m - l + 1;
    int n2 = r - m;
 
    /* create temp arrays */
    float L[n1], R[n2];
 
    /* Copy data to temp arrays L[] and R[] */
    for (i = 0; i < n1; i++)
        L[i] = arr[l + i];
    for (j = 0; j < n2; j++)
        R[j] = arr[m + 1 + j];
 
    /* Merge the temp arrays back into arr[l..r]*/
    i = 0; // Initial index of first subarray
    j = 0; // Initial index of second subarray
    k = l; // Initial index of merged subarray
    while (i < n1 && j < n2) {
        if (L[i] <= R[j]) {
            arr[k] = L[i];
            i++;
        }
        else {
            arr[k] = R[j];
            j++;
        }
        k++;
    }
 
    /* Copy the remaining elements of L[], if there
    are any */
    while (i < n1) {
        arr[k] = L[i];
        i++;
        k++;
    }
 
    /* Copy the remaining elements of R[], if there
    are any */
    while (j < n2) {
        arr[k] = R[j];
        j++;
        k++;
    }
}

/* l is for left index and r is right index of the
sub-array of arr to be sorted */
inline void mergeSort(float arr[], int l, int r)
{
    if (l < r) {
        // Same as (l+r)/2, but avoids overflow for
        // large l and h
        int m = l + (r - l) / 2;
 
        // Sort first and second halves
        mergeSort(arr, l, m);
        mergeSort(arr, m + 1, r);
 
        merge(arr, l, m, r);
    }
}

// Ending mergeSort


//geopat single priority queue
void* calculate_eyclidian_distances(void *rfdata)
{     
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    unsigned long ts_per_thread = input_data->timeSeriesNum/maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;

    
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    unsigned long j = 0;
    for(unsigned long j = my_ts_start;j < my_ts_end;j++){
        float distance = calculate_eyclidian_distance(input_data->index, input_data->ts, bsfdisntance,j);
        
        input_data->distances_array[j] = distance;
    }


    for(unsigned long j = 0 ; j < input_data->timeSeriesNum ; j++){
        if(*input_data->finished_buffer == 1){
            break;
        }
        if(input_data->distances_array[j] == 0 ){
            float distance = calculate_eyclidian_distance(input_data->index, input_data->ts, bsfdisntance,j);
            input_data->distances_array[j] = distance;
        }
    }
    *input_data->finished_buffer = 1;

}

void visitNode(isax_node_single_buffer *node){
    if(node == NULL){
        return;
    }
    visitNode(node->node->left_child);
    node->node->processed = 1;
    visitNode(node->node->right_child);
}



void process_node(isax_node_single_buffer *rootNode,isax_node_single_buffer *node,int node_number, isax_index *index,MESSI_workerdata_ekosmas_lf *input_data,ts_type *paa,float bsfdisntance ){
    if(node != NULL && node->node->isax_cardinalities !=NULL && node->node->isax_values != NULL){
        if(node->node->is_leaf && node->node->buffer->partial_buffer_size == 0){
            return;
        }
                        
        float distance =  minidist_paa_to_isax(paa, node->node->isax_values,node->node->isax_cardinalities,
                        index->settings->sax_bit_cardinality,
                        index->settings->sax_alphabet_cardinality,
                        index->settings->paa_segments,
                        MINVAL, MAXVAL,
                        index->settings->mindist_sqrt); 
                                                           
        if(distance < bsfdisntance){   
                query_result * mindist_result = malloc(sizeof(query_result));
                mindist_result->node= node;
                mindist_result->distance = distance;
                unsigned long  k = __sync_fetch_and_add(&pq_size, 1);
                insert(input_data->main_queue,k, (void *)mindist_result);
                node->node->processed = 1;              
        }
        else{   //prouning
            int skipNodes = node_number + node->subTreeNodesNum +1;
            while(rootNode->node_counter < skipNodes ){
                node_number = __sync_fetch_and_add(&rootNode->node_counter, 1);
            }
            visitNode(node);              
        }  
    }            
}

void help_node(isax_node_single_buffer *node, isax_index *index,MESSI_workerdata_ekosmas_lf *input_data,ts_type *paa,float bsfdisntance ){
    
    if(node == NULL){
        return;
    }

    help_node(node->node->left_child,index,input_data,paa,bsfdisntance );
    if(node->node->processed == 0){

        if(node != NULL && node->node->isax_cardinalities !=NULL && node->node->isax_values != NULL ){
            if(node->node->is_leaf && node->node->buffer->partial_buffer_size == 0){
                return;
            }
                            
            float distance =  minidist_paa_to_isax(paa, node->node->isax_values,node->node->isax_cardinalities,
                            index->settings->sax_bit_cardinality,
                            index->settings->sax_alphabet_cardinality,
                            index->settings->paa_segments,
                            MINVAL, MAXVAL,
                            index->settings->mindist_sqrt); 
                                                            
            if(distance < bsfdisntance){    
                if(node->node->is_leaf){
                    query_result * mindist_result = malloc(sizeof(query_result));
                    mindist_result->node= node;
                    mindist_result->distance = distance;
                    unsigned long  k = __sync_fetch_and_add(&pq_size, 1);
                    insert(input_data->main_queue,k, (void *)mindist_result);
                }
            }
        } 
        node->node->processed = 1;
    }
   help_node(node->node->right_child,index,input_data,paa,bsfdisntance );            
}


//geopat single priority queue
void* exact_search_worker_inmemory_single_priority_queue_geopat(void *rfdata)
{     
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;

    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

   // struct IsaxPQ *queue = input_data->main_queue[0];

   
    unsigned long j = 0;
    int k = 0 ;
    isax_node_single_buffer *rootNode;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    int count = 0 ;
    for(k = 0 ; k < number_of_subtrees; k++){  // For each subtree get ith node and add to queue
        if(current_fbl_node->node[k] != NULL){
           rootNode = current_fbl_node->node[k];
            while (1)
            {   
                j = __sync_fetch_and_add(&rootNode->node_counter, 1);
                if (j > (rootNode->subTreeNodesNum + 1))
                {
                    break;
                }
                else{
                    isax_node_single_buffer *node = getIthNode(rootNode,j);
                    process_node(rootNode,node,j,index,input_data,paa,bsfdisntance);           
                }          
            }
           help_node(rootNode,index,input_data,paa,bsfdisntance);
           // rootNode->rootProcessedSubtree = 1;
        }
    }
   
      
    // B. Processing priority queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    process_queue_node_geopat(input_data);

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

}

void* exact_search_worker_inmemory_40_priority_queue_geopat(void *rfdata)
{     
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;

    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

   // struct IsaxPQ *queue = input_data->main_queue[0];

   
    unsigned long j = 0;
    int k = 0 ;
    isax_node_single_buffer *rootNode;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    int count = 0 ;
    for(k = 0 ; k < number_of_subtrees; k++){  // For each subtree get ith node and add to queue
        if(current_fbl_node->node[k] != NULL){
           rootNode = current_fbl_node->node[k];
            while (1)
            {   
                j = __sync_fetch_and_add(&rootNode->node_counter, 1);
                if (j > (rootNode->subTreeNodesNum + 1))
                {
                    break;
                }
                else{
                    isax_node_single_buffer *node = getIthNode(rootNode,j);
                    if(node != NULL ){
                        if(node->node->is_leaf && node->node->buffer->partial_buffer_size == 0){
                            continue;
                        }
                        
                        float distance =  minidist_paa_to_isax(paa, node->node->isax_values,node->node->isax_cardinalities,
                                                    index->settings->sax_bit_cardinality,
                                                    index->settings->sax_alphabet_cardinality,
                                                    index->settings->paa_segments,
                                                    MINVAL, MAXVAL,
                                                    index->settings->mindist_sqrt); 
                                                           
                        if(distance < bsfdisntance){    
                            query_result * mindist_result = malloc(sizeof(query_result));
                            mindist_result->node= node;
                            mindist_result->distance = distance;
                            unsigned long  k = __sync_fetch_and_add(&pq_size, 1);
                            pq_t *pq = input_data->main_queues[input_data->workernumber % 40];
                            insert(pq,k, (void *)mindist_result);
                            count++;

                       }
                        else{   //prouning
                            int skipNodes = j + node->subTreeNodesNum +1;
                            while(rootNode->node_counter < skipNodes ){
                                j = __sync_fetch_and_add(&rootNode->node_counter, 1);
                            }
                            
                        }  

                    }                 
                }          
            }
        }
    }
   
      
    // B. Processing priority queue
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }
    process_queue_node_geopat2(input_data);

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
    }

}



 isax_node * getIthNode2(isax_node *node , int node_number ){
    isax_node *tmp = node;
    isax_node *result = NULL;
    if(tmp == NULL) return NULL;
    if(node_number == 1){
        return tmp;
    }
    else{
        return NULL;
    }
    result = getIthNode2(tmp->left_child,node_number-1);
    if(result != NULL) return result;
    result = getIthNode2(tmp->right_child,node_number-1);
    if(result != NULL) return result;
}



isax_node_single_buffer * getIthNode(isax_node_single_buffer *node , int node_number ){
    isax_node_single_buffer *tmp = node;
    if(tmp == NULL) return NULL;
    if(tmp->subTreeNodesNum + 1 < node_number){
        if(tmp->subTreeNodesNum == 0 ){
            return tmp;
        }
        return NULL;
    }
    else{
        while(1){
            
            if(node_number == 1){
              return tmp;
            }


            if(node_number > tmp->leftSubtreeCounter + 1){
                node_number = node_number - (tmp->leftSubtreeCounter + 1);
                tmp = (isax_node_single_buffer *)tmp->node->right_child;
            }
            else{
                tmp = (isax_node_single_buffer *)tmp->node->left_child;
                node_number = node_number - 1;
            }
        }
    }
}

void process_node_geopat_fresh(isax_node_single_buffer *rootNode,isax_node_single_buffer *node,int node_number, isax_index *index,MESSI_workerdata_ekosmas_lf *input_data,ts_type *paa,float bsfdisntance,query_result ***pq_data, int *tnumber, volatile unsigned long *next_queue_data_pos ){
    if(node != NULL && node->node->isax_cardinalities !=NULL && node->node->isax_values != NULL ){
                        
        float distance =  minidist_paa_to_isax(paa, node->node->isax_values,node->node->isax_cardinalities,
                        index->settings->sax_bit_cardinality,
                        index->settings->sax_alphabet_cardinality,
                        index->settings->paa_segments,
                        MINVAL, MAXVAL,
                        index->settings->mindist_sqrt); 

                                                           
        if(distance < bsfdisntance){    
            if(node->node->is_leaf){
                query_result * mindist_result = malloc(sizeof(query_result));
                mindist_result->node = node;
                mindist_result->distance = distance; 
                // pqueue_insert(pq, mindist_result);
                unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
                pq_data[*tnumber][queue_data_pos] = mindist_result;
                *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
            }
            node->node->processed = 1;  
        }
        else{   //prouning
            int skipNodes = node_number + node->subTreeNodesNum +1;
            while(rootNode->node_counter < skipNodes ){
                node_number = __sync_fetch_and_add(&rootNode->node_counter, 1);
            }
            visitNode(node);
                            
        }  
         
    }            
}

void help_node_geopat_fresh(isax_node *node, isax_index *index,MESSI_workerdata_ekosmas_lf *input_data,ts_type *paa,float bsfdisntance,query_result ***pq_data, int *tnumber, volatile unsigned long *next_queue_data_pos ){
    if(node == NULL){
        return;
    }
    help_node_geopat_fresh(node->left_child,index,input_data,paa,bsfdisntance,pq_data, tnumber,next_queue_data_pos);
    if(node->processed != 1){
        if(node != NULL && node->isax_cardinalities !=NULL && node->isax_values != NULL ){
                        
            float distance =  minidist_paa_to_isax(paa, node->isax_values,node->isax_cardinalities,
                            index->settings->sax_bit_cardinality,
                            index->settings->sax_alphabet_cardinality,
                            index->settings->paa_segments,
                            MINVAL, MAXVAL,
                            index->settings->mindist_sqrt); 

                                                            
            if(distance < bsfdisntance){    
                if(node->is_leaf){
                    query_result * mindist_result = malloc(sizeof(query_result));
                    mindist_result->node = node;
                    mindist_result->distance = distance; 
                    // pqueue_insert(pq, mindist_result);
                    unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
                    pq_data[*tnumber][queue_data_pos] = mindist_result;
                    *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
                }
            
            } 
        }  
        node->processed = 1;
    }
    
    help_node_geopat_fresh(node->right_child,index,input_data,paa,bsfdisntance,pq_data, tnumber,next_queue_data_pos);          
}




// ekosmas-lf version - MIXED - Version with 80 sorted arrays and helping during sorted array processing, only after a helper exists.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_only_after_helper(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    while (1) {

    /*    for(int i = 0 ; i <input_data->amountnode;i++){
             current_root_node = input_data->nodelist[i];
             while(1){
                int current_node_number = __sync_fetch_and_add(&current_root_node->node_counter, 1);
                if (current_node_number > (current_root_node->subTreeNodesNum + 1)){
                        break;
                }
                isax_node *node = getIthNode(current_root_node,current_node_number);
                if(node != NULL && (node->isax_values != NULL || node->isax_cardinalities != NULL) ){
                    process_node_geopat_fresh(current_root_node,node,current_node_number,index,input_data,paa,bsfdisntance,input_data->allpq_data, &tnumber, input_data->next_queue_data_pos);
                  //  add_to_queue_data_lf(paa,node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
                }
             }
        }
        */
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
            
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    /*for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
        current_root_node = input_data->nodelist[current_root_node_number];
          help_node_geopat_fresh(current_root_node,index,input_data,paa,bsfdisntance,input_data->allpq_data, &tnumber, input_data->next_queue_data_pos);
    }
*/
    // A.1. Help unprocessed subtrees
    if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    
    }
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my sorted array
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_sorted_array_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my sorted array
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exists, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        int ret_val;
        // printf ("[MY] executing while - starttt\n"); fflush(stdout);
        while ( !input_data->helper_queue_exist[input_data->workernumber] &&
                (ret_val = process_sorted_array_element_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
            ;
        }

        // printf ("[MY] executing while - end\n"); fflush(stdout);

        // if (!ret_val) {
        //     printf ("[MY] my sorted array processed!\n"); fflush(stdout);
        // }

        if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
            input_data->queue_finished[input_data->workernumber] = 1;
        }

        // B.2. if helpers exist and my sorted array has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
            help_sorted_array(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
            // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        }
        // else {
        //     printf ("[MY] I help nobody!!!\n"); fflush(stdout);
        // }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other sorted arrays to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_sorted_array_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
                // printf ("Created sorted array [%d] with size [%d]\n", i, input_data->allpq[i]->size); fflush(stdout);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished sorted arrays
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            // printf ("[OTHERS] executing help_sorted_array for array [%d]  - starttt\n", i); fflush(stdout);
            help_sorted_array(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
            // printf ("[OTHERS] executing help_sorted_array for array [%d] - enddd\n", i); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   
}


// ekosmas-lf version - MIXED - Version with 80 sorted arrays and helping during sorted array processing from the beggining.
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_mixed_sorted_array_full_helping(void *rfdata)
{   
    threadPin(((MESSI_workerdata_ekosmas_lf*)rfdata)->workernumber, maxquerythread);

    isax_node *current_root_node;

    MESSI_workerdata_ekosmas_lf *input_data = (MESSI_workerdata_ekosmas_lf*)rfdata;

    isax_index *index = input_data->index;
    ts_type *paa = input_data->paa;
    
    volatile query_result *bsf_result = *(input_data->bsf_result_p);
    float bsfdisntance = bsf_result->distance;
    const int query_id = input_data->query_id;
  
    // A. Populate Queues - Lock-Free Version with Load Balancing (threads are inserting into data queues in a round robin manner!)
    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_START
    }
    
    int tnumber = rand() % NUM_PRIORITY_QUEUES;
    while (1) {
            int current_root_node_number = __sync_fetch_and_add(input_data->node_counter, 1);
            if(current_root_node_number >= input_data->amountnode) {
                break;
            }

            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.1. Help unprocessed subtrees
    if (!DO_NOT_HELP) {
        for (int current_root_node_number = 0; current_root_node_number < input_data->amountnode; current_root_node_number++) {
            current_root_node = input_data->nodelist[current_root_node_number];
            add_to_queue_data_lf(paa, current_root_node, index, bsfdisntance, input_data->allpq_data, &tnumber, input_data->next_queue_data_pos, query_id);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
    }    

    // printf ("data_queue %d has size [%d]\n", input_data->workernumber, input_data->next_queue_data_pos[input_data->workernumber]); fflush(stdout);

    // A.2. Create my sorted array
    if (!input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES] && input_data->next_queue_data_pos[input_data->workernumber % NUM_PRIORITY_QUEUES]) {
        create_sorted_array_from_data_queue(input_data->workernumber % NUM_PRIORITY_QUEUES, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    }

    // B. Processing my sorted array
    int checks = 0;                                                                     // This is just for debugging. It can be removed!

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
    }

    // B.1. while no helper exists, execute fast path: while a candidate queue node with smaller distance exists, compute actual distances
    pqueue_t *my_pq = input_data->allpq[input_data->workernumber % NUM_PRIORITY_QUEUES];
    if (my_pq) {
        // int ret_val;
        // // printf ("[MY] executing while - starttt\n"); fflush(stdout);
        // while ( !input_data->helper_queue_exist[input_data->workernumber] &&
        //         (ret_val = process_sorted_array_element_ekosmas_lf(input_data, my_pq, input_data->workernumber, &checks, input_data->parallelism_in_subtree))) {         
        //     ;
        // }

        // // printf ("[MY] executing while - end\n"); fflush(stdout);

        // // if (!ret_val) {
        // //     printf ("[MY] my sorted array processed!\n"); fflush(stdout);
        // // }

        // if (!ret_val && !input_data->queue_finished[input_data->workernumber]) {
        //     input_data->queue_finished[input_data->workernumber] = 1;
        // }

        // B.2. if helpers exist and my sorted array has not yet finished, execute slow path
        if (input_data->helper_queue_exist[input_data->workernumber] && !input_data->queue_finished[input_data->workernumber]) {
            // printf ("[MY] executing help_queue - starttt\n"); fflush(stdout);
            help_sorted_array(input_data, my_pq, input_data->workernumber % NUM_PRIORITY_QUEUES, &checks, input_data->parallelism_in_subtree);
            // printf ("[MY] executing help_queue - endddd\n"); fflush(stdout);
        }
        // else {
        //     printf ("[MY] I help nobody!!!\n"); fflush(stdout);
        // }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_TIME_END
        COUNT_QUEUE_FILL_TIME_START
        COUNT_QUEUE_FILL_HELP_TIME_START
    }

    // A.3. Help other sorted arrays to be created
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (!input_data->allpq[i] && input_data->next_queue_data_pos[i]) {
                create_sorted_array_from_data_queue(i, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
                // printf ("Created sorted array [%d] with size [%d]\n", i, input_data->allpq[i]->size); fflush(stdout);
            }
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_FILL_HELP_TIME_END
        COUNT_QUEUE_FILL_TIME_END
        COUNT_QUEUE_PROCESS_TIME_START
        COUNT_QUEUE_PROCESS_HELP_TIME_START
    }

    // B.3. Help non-finished sorted arrays
    if (!DO_NOT_HELP) {
        for (int i = 0; i < NUM_PRIORITY_QUEUES; i++)
        {
            if (input_data->queue_finished[i] || !input_data->allpq[i]) {                                           // if queue is completed, skip helping it 
                continue;
            }

            if (!input_data->helper_queue_exist[i]) {                                                               // inform that helpers exist 
                input_data->helper_queue_exist[i] = 1;
            }

            // printf ("[OTHERS] executing help_sorted_array for array [%d]  - starttt\n", i); fflush(stdout);
            help_sorted_array(input_data, input_data->allpq[i], i, &checks, input_data->parallelism_in_subtree);           // help it, by executing slow path
            // printf ("[OTHERS] executing help_sorted_array for array [%d] - enddd\n", i); fflush(stdout);
        }
    }

    if (input_data->workernumber == 0) {
        COUNT_QUEUE_PROCESS_HELP_TIME_END
        COUNT_QUEUE_PROCESS_TIME_END
    }


    // EKOSMAS: UNSAFE!
    // // C. Free any element left in my queue
    // // printf("exact_search_worker_inmemory_hybridpqueue_ekosmas_lf: FREE REST QUEUE ELEMENTS\n");fflush(stdout);
    // query_result *n;
    // while(n = pqueue_pop(my_pq))
    // {
    //     free(n);
    // }   
}

// ekosmas-lf version
void add_to_queue_data_lf(float *paa, isax_node *node, isax_index *index, float bsf, query_result ***pq_data, int *tnumber, volatile unsigned long *next_queue_data_pos, const int query_id)
{   

    if (node->processed > query_id) {         // it can only be: node->processed == query_id+1
        return;
    }


    //COUNT_CAL_TIME_START
    // ??? EKOSMAS: Why not using SIMD version of the following function?
    // Botao will send a correct SIMD version of the following function
    // calculate lower bound distance
    float distance =  minidist_paa_to_isax(paa, node->isax_values,
                                            node->isax_cardinalities,
                                            index->settings->sax_bit_cardinality,
                                            index->settings->sax_alphabet_cardinality,
                                            index->settings->paa_segments,
                                            MINVAL, MAXVAL,
                                            index->settings->mindist_sqrt);
    //COUNT_CAL_TIME_END

    if(distance < bsf && node->processed <= query_id)
    {
        if (node->is_leaf) 
        {   
            query_result * mindist_result = malloc(sizeof(query_result));
            mindist_result->node = node;
            mindist_result->distance = distance;

            
            // pqueue_insert(pq, mindist_result);
            unsigned long queue_data_pos = __sync_fetch_and_add(&(next_queue_data_pos[*tnumber]),1);
            pq_data[*tnumber][queue_data_pos] = mindist_result;
            *tnumber=(*tnumber+1)%NUM_PRIORITY_QUEUES;
        }
        else
        {   
            if (((isax_node*)node->left_child)->isax_cardinalities != NULL && ((isax_node*)node->left_child)->processed <= query_id)                        // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                add_to_queue_data_lf(paa,((isax_node*)node->left_child), index, bsf, pq_data, tnumber, next_queue_data_pos, query_id);
            }
            if (((isax_node*)node->right_child)->isax_cardinalities != NULL && ((isax_node*)node->right_child)->processed <= query_id)                      // ??? EKOSMAS: Can this be NULL, while node is not leaf???
            {
                add_to_queue_data_lf(paa, ((isax_node*)node->right_child), index, bsf, pq_data, tnumber, next_queue_data_pos, query_id);
            }
        }
    }

    // mark node as processed
    if (node->processed <= query_id) {
        node->processed = query_id + 1;
    }
}


