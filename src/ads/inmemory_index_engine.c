  
//
//  Updated by Eleftherios Kosmas on May 2020.
//

#define _GNU_SOURCE

#ifdef VALUES

#include <values.h>
#endif
#include "kdtree.h"
#include <float.h>
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <stdbool.h>
#include "ads/isax_query_engine.h"
#include "ads/inmemory_index_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"
#include <sched.h>
#include <unistd.h>

next_ts_group *next_ts_group_read_in_block;
next_ts_group *next_ts_group_read_in_block_sum_buffer;
volatile unsigned char all_blocks_processed;
volatile unsigned char all_RecBufs_processed;
volatile unsigned char *block_helper_exist;
volatile unsigned char *block_helper_exist_sum_buffer;
volatile unsigned char *block_helpers_num;
volatile unsigned char *recBuf_helpers_num;

inline void backoff_delay_char(unsigned long backoff, volatile unsigned char *stop)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_copy(unsigned long backoff, isax_node *volatile *stop)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void backoff_delay_lockfree_subtree_parallel(unsigned long backoff, volatile unsigned char *stop)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}

inline void threadPin(int pid, int max_threads)
{
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

    CPU_SET((cpu_id % 4) * 10 + (cpu_id % 40) / 4 + (cpu_id / 40) * 40, &mask); // SOCKETS PINNING - Vader

    int ret = sched_setaffinity(0, len, &mask);
    if (ret == -1)
        perror("sched_setaffinity");
}

long int count_ts_in_nodes(isax_node *root_node, const char parallelism_in_subtree, const char recBuf_helpers_exist)
{
    long int my_subtree_nodes = 0;

    if (!root_node->is_leaf)
    {
        my_subtree_nodes = count_ts_in_nodes(root_node->left_child, parallelism_in_subtree, recBuf_helpers_exist);
        my_subtree_nodes += count_ts_in_nodes(root_node->right_child, parallelism_in_subtree, recBuf_helpers_exist);
        return my_subtree_nodes;
    }
    else
    {
        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && root_node->recBuf_leaf_helpers_exist))
        {
            if (root_node->fai_leaf_size == 0)
            {
                return root_node->leaf_size;
            }
            else if (root_node->fai_leaf_size < root_node->leaf_size)
            {
                printf("root_node->fai_leaf_size < root_node->leaf_size  !!!!\n");
                fflush(stdout);
            }
            return root_node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW)
        {
            return root_node->buffer->partial_buffer_size;
        }
        else
        {
            return root_node->leaf_size;
        }
    }
}


long int count_ts_in_nodes_geopat(isax_node_single_buffer *root_node,int parallelism_in_subtree)
{
    long int my_subtree_nodes = 0;

    if (!root_node->node->is_leaf)
    {
        my_subtree_nodes = count_ts_in_nodes_geopat(root_node->node->left_child,parallelism_in_subtree);
        my_subtree_nodes += count_ts_in_nodes_geopat(root_node->node->right_child,parallelism_in_subtree);
        return my_subtree_nodes;
    }
    else
    {   
        //if(parallelism_in_subtree == 0){
        //    return root_node->buffer_rc.s.position;
        //}
        //else{
           return root_node->node->buffer->partial_buffer_size;
        //}
        
    }
    
}

inline void check_validity(isax_index *index, long int ts_num)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
        }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees)
        {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees)
        {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10)
        {
            cnt_1_10++;
        }
        else if (tmp_num >= 10 && tmp_num < 100)
        {
            cnt_10_100++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000)
        {
            cnt_100_1000++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000)
        {
            cnt_1000_10000++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000)
        {
            cnt_10000_100000++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000)
        {
            cnt_100000_1000000++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000)
        {
            cnt_1000000_10000000++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}
inline void check_validity_ekosmas(isax_index *index, long int ts_num)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }
        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, NO_PARALLELISM_IN_SUBTREE, 0);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees)
        {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees)
        {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10)
        {
            cnt_1_10++;
        }
        else if (tmp_num >= 10 && tmp_num < 100)
        {
            cnt_10_100++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000)
        {
            cnt_100_1000++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000)
        {
            cnt_1000_10000++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000)
        {
            cnt_10000_100000++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000)
        {
            cnt_100000_1000000++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000)
        {
            cnt_1000000_10000000++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);
}
void check_validity_ekosmas_lf(isax_index *index, long int ts_num, const char parallelism_in_subtree)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        // int tmp_count = 0;
        for (int k = 0; k < maxquerythread; k++)
        {
            // tmp_count += current_fbl_node->buffer_size[k];
            ts_in_RecBufs_cnt += current_fbl_node->buffer_size[k];
        }

        // printf("RecBuf[%d] contains [%d] iSAX summarries\n", i, tmp_count);
    }

    // print difference with actual total time series in raw file
     printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf *)(index->fbl))->soft_buffers[i];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        non_empty_subtrees_cnt++;
        long int tmp_num = count_ts_in_nodes((isax_node *)current_fbl_node->node, parallelism_in_subtree, current_fbl_node->recBuf_helpers_exist);
        ts_in_tree_cnt += tmp_num;
        if (tmp_num < min_ts_in_subtrees)
        {
            min_ts_in_subtrees = tmp_num;
        }
        else if (tmp_num > max_ts_in_subtrees)
        {
            max_ts_in_subtrees = tmp_num;
        }

        // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

        if (tmp_num < 10)
        {
            cnt_1_10++;
        }
        else if (tmp_num >= 10 && tmp_num < 100)
        {
            cnt_10_100++;
        }
        else if (tmp_num >= 100 && tmp_num < 1000)
        {
            cnt_100_1000++;
        }
        else if (tmp_num >= 1000 && tmp_num < 10000)
        {
            cnt_1000_10000++;
        }
        else if (tmp_num >= 10000 && tmp_num < 100000)
        {
            cnt_10000_100000++;
        }
        else if (tmp_num >= 100000 && tmp_num < 1000000)
        {
            cnt_100000_1000000++;
        }
        else if (tmp_num >= 1000000 && tmp_num < 10000000)
        {
            cnt_1000000_10000000++;
        }

        // if (recBuf_helpers_num[i]) {
        //     printf("RecBuf [%d] was helped by [%d] threads and contains [%d] nodes \n", i, recBuf_helpers_num[i], tmp_num);
        // }
    }

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
     printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);

    // check that all data series have been processed!
    for (int ts_id = 0; ts_id < ts_num; ts_id++)
    {
        if (!ts_processed[ts_id])
        {
            printf("--- ERROR : Time series with id [%d] has not been processed!!!! ---\n", ts_id);
            fflush(stdout);
        }
    }

    if (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE)
    {
        return;
    }

    // check that all iSAX summaries have been iserted into index tree
    unsigned long num_iSAX_processed_from_RecBufs = 0;
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf *)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized)
        {
            continue;
        }

        for (int j = 0; j < maxquerythread; j++)
        {
            for (int k = 0; k < current_fbl_node->buffer_size[j]; k++)
            {
                if (!current_fbl_node->iSAX_processed[j][k])
                {
                    printf("--- ERROR : iSAX summary of [%d] recBuf in position ([%d],[%d]) has not been processed!!!! ---", i, j, k);
                    fflush(stdout);
                }
            }
            num_iSAX_processed_from_RecBufs += current_fbl_node->buffer_size[j];
        }
    }

     printf ("Processed [%d] iSAX summaries from RecBufs\n", num_iSAX_processed_from_RecBufs);
}


 void check_validity_lf_geopat(isax_index *index, long int ts_num,int parallelism_in_subtree)
{

    // print the number of threads helped each block (not required for validity)
    // unsigned long total_blocks = ts_num/read_block_length;
    // for (int i=0; i < total_blocks; i++) {
    //     if (block_helpers_num[i]) {
    //         printf("Block [%d] was helped by [%d] threads\n", i, block_helpers_num[i]);
    //     }
    // }

    // count the total number of time series stored into receive buffers
    ts_in_RecBufs_cnt = 0;
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);  
   // ts_in_RecBufs_cnt = *current_fbl_node->buffer_size;
        

    // print difference with actual total time series in raw file
    // printf ("Total series in RecBufs = [%d] which are [%d] more than total series in raw file\n", ts_in_RecBufs_cnt, ts_in_RecBufs_cnt - ts_num);

    // count the total number of time series stored into tree index
    ts_in_tree_cnt = 0;
    non_empty_subtrees_cnt = 0;
    min_ts_in_subtrees = ts_num;
    max_ts_in_subtrees = 0;
    int cnt_1_10 = 0;
    int cnt_10_100 = 0;
    int cnt_100_1000 = 0;
    int cnt_1000_10000 = 0;
    int cnt_10000_100000 = 0;
    int cnt_100000_1000000 = 0;
    int cnt_1000000_10000000 = 0;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    for(int i = 0 ; i < number_of_subtrees; i++){
        if(current_fbl_node->node[i] != NULL){
            non_empty_subtrees_cnt++;
            long int tmp_num = count_ts_in_nodes_geopat((isax_node_single_buffer *)current_fbl_node->node[i],parallelism_in_subtree);
            ts_in_tree_cnt += tmp_num;
            if (tmp_num < min_ts_in_subtrees)
            {
                min_ts_in_subtrees = tmp_num;
            }
            else if (tmp_num > max_ts_in_subtrees)
            {
                max_ts_in_subtrees = tmp_num;
            }

            // printf("Subtree[%d] contains [%d] nodes\n", i, tmp_num);

            if (tmp_num < 10)
            {
                cnt_1_10++;
            }
            else if (tmp_num >= 10 && tmp_num < 100)
            {
                cnt_10_100++;
            }
            else if (tmp_num >= 100 && tmp_num < 1000)
            {
                cnt_100_1000++;
            }
            else if (tmp_num >= 1000 && tmp_num < 10000)
            {
                cnt_1000_10000++;
            }
            else if (tmp_num >= 10000 && tmp_num < 100000)
            {
                cnt_10000_100000++;
            }
            else if (tmp_num >= 100000 && tmp_num < 1000000)
            {
                cnt_100000_1000000++;
            }
            else if (tmp_num >= 1000000 && tmp_num < 10000000)
            {
                cnt_1000000_10000000++;
            }
        }     
    }
    printf("TSNUM = %ld , ts_in_tree_cnt = %ld\n",ts_num,ts_in_tree_cnt );
    if(ts_in_tree_cnt < ts_num ){
       // printf("Nodes count in tree %ld ,Nodes in buffer %ld ",ts_in_tree_cnt,*current_fbl_node->buffer_size );
    }
    else{
        printf("Tree node equal to buffer size");
    }
 

    // printf("\nThere exist [%d] subtrees with 1-9 nodes\n", cnt_1_10);
    // printf("There exist [%d] subtrees with 10-99 nodes\n", cnt_10_100);
    // printf("There exist [%d] subtrees with 100-999 nodes\n", cnt_100_1000);
    // printf("There exist [%d] subtrees with 1000-9999 nodes\n", cnt_1000_10000);
    // printf("There exist [%d] subtrees with 10000-99999 nodes\n", cnt_10000_100000);
    // printf("There exist [%d] subtrees with 100000-999999 nodes\n", cnt_100000_1000000);
    // printf("There exist [%d] subtrees with 1000000-9999999 nodes\n", cnt_1000000_10000000);

    // compare numbers of time series stored into receive buffers and tree index. They have to be the same!!!
    // printf ("Total series in Tree = [%d] which are [%d] more than total series in RecBufs\n", ts_in_tree_cnt, ts_in_tree_cnt - ts_in_RecBufs_cnt);

    // check that all iSAX summaries have been iserted into index tree
    // printf ("Processed [%d] iSAX summaries from RecBufs\n", num_iSAX_processed_from_RecBufs);
}




// reads data from file
// place data in memory
// create the buffers
// void index_creation_pRecBuf_new_botao
void index_creation_pRecBuf_new(const char *ifilename, long int ts_num, isax_index *index)
{
    // fprintf(stderr, ">>> Indexing: %s\n", ifilename);
    FILE *ifile;
    COUNT_INPUT_TIME_START
    ifile = fopen(ifilename, "rb");
    COUNT_INPUT_TIME_END

    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }
    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);
    file_position_type total_records = sz / index->settings->ts_byte_size;
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    {
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    index->sax_file = NULL;

    long int ts_loaded = 0;
    unsigned long shared_start_number = 0;
    int i;
    int node_counter = 0;
    pthread_t threadid[maxquerythread];
    buffer_data_inmemory *input_data = malloc(sizeof(buffer_data_inmemory) * (maxquerythread));
    rawfile = malloc(sizeof(ts_type) * index->settings->timeseries_size * ts_num);
    index->sax_cache = malloc(sizeof(sax_type) * index->settings->paa_segments * ts_num);
    pthread_barrier_t lock_barrier1, lock_barrier2;
    pthread_barrier_init(&lock_barrier1, NULL, maxquerythread + 1);
    pthread_barrier_init(&lock_barrier2, NULL, maxquerythread + 1);
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    pthread_mutex_t lock_record = PTHREAD_MUTEX_INITIALIZER, lockfbl = PTHREAD_MUTEX_INITIALIZER, lock_index = PTHREAD_MUTEX_INITIALIZER,
                    lock_firstnode = PTHREAD_MUTEX_INITIALIZER, lock_disk = PTHREAD_MUTEX_INITIALIZER;

    destroy_fbl(index->fbl);
    index->fbl = (struct first_buffer_layer *)initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
                                                                 pow(2, index->settings->paa_segments),
                                                                 index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);
    // set the thread on decided cpu

    // COUNT_OUTPUT_TIME_START
    int nodeid[index->fbl->number_of_buffers];
    int nodesize[index->fbl->number_of_buffers];

    for (i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_fbl = &lockfbl;
        input_data[i].lock_record = &lock_record;
        input_data[i].lock_firstnode = &lock_firstnode;
        input_data[i].lock_index = &lock_index;
        input_data[i].ts = rawfile;
        input_data[i].lock_disk = &lock_disk;
        input_data[i].workernumber = i;
        input_data[i].total_workernumber = maxquerythread;
        input_data[i].start_number = i * (ts_num / maxquerythread);
        input_data[i].shared_start_number = &shared_start_number;
        input_data[i].stop_number = ts_num;
        input_data[i].node_counter = &node_counter;
        input_data[i].lock_barrier1 = &lock_barrier1;
        input_data[i].lock_barrier2 = &lock_barrier2;
        input_data[i].nodeid = nodeid;
    }
    for (i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new, (void *)&(input_data[i]));
    }

    pthread_barrier_wait(&lock_barrier1);

    // wait for the finish of other threads
    for (i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }
    __sync_fetch_and_add(&(index->total_records), ts_num);
    index->sax_cache_size = index->total_records;
    fclose(ifile);
    // fprintf(stderr, ">>> Finished indexing\n");
    free(input_data);
    // printf(" the sax point is %d\n",index->first_node->isax_cardinalities[0]);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity(index, ts_num);
}

///////////////////////////
// Inorder traversal
void inorderTraversal(isax_node* root) {
  if (root == NULL) return;
  inorderTraversal(root->left_child);
  if(root->leftmost_leaf!=NULL && root->rightmost_leaf != NULL){
    printf("leftest %d rightest %d \n", root->leftmost_leaf->leaf_id,root->rightmost_leaf->leaf_id);
  }else if(root->leftmost_leaf==NULL && root->rightmost_leaf==NULL){
    printf("both null isleaf:%d\n",root->is_leaf);
  }else if(root->leftmost_leaf==NULL){
    printf("leftest is NULL rightest %d \n", root->rightmost_leaf->leaf_id);
  }else{
    printf("leftest is %d rightest is NULL\n", root->leftmost_leaf->leaf_id);
  }
  
  inorderTraversal(root->right_child);
}

void inorderTraversalCheckForLRpointers(isax_node* root) {
  if (root == NULL) return;
  inorderTraversalCheckForLRpointers(root->left_child);
    

   if(root->leftmost_leaf!=NULL && root->rightmost_leaf != NULL){
    //printf("leftest %d rightest %d \n", root->leftmost_leaf->leaf_id,root->rightmost_leaf->leaf_id);
   }else if(root->leftmost_leaf==NULL && root->rightmost_leaf==NULL && root->is_leaf == 0){
     printf("both null \n");
   }else if(root->leftmost_leaf==NULL && root->rightmost_leaf==NULL && root->is_leaf == 1){

   }else if(root->leftmost_leaf==NULL){
    printf("leftest is NULL rightest %d \n", root->rightmost_leaf->leaf_id);
   }else if(root->rightmost_leaf==NULL){
    printf("leftest is %d rightest is NULL\n", root->leftmost_leaf->leaf_id);
   } 
  

  inorderTraversalCheckForLRpointers(root->right_child);
}
///////////////////////////
void index_creation_pRecBuf_new_ekosmas_func(const char *ifilename, long int ts_num, isax_index *index, char embarrassingly_parallel, const char parallelism_in_subtree,const char*afilename)///////////////////////
{
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    
    
    ////////////////////////////////////////////////////
    FILE *afile;
    afile = fopen(afilename, "rb");
    if (afile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", afilename);
        exit(-1);
    }

    fseek(afile, 0L, SEEK_END);
    file_position_type sz2 = (file_position_type)ftell(afile);              // sz2 = size in bytes
    file_position_type total_records2 = sz2 / (sizeof(attribute_type)*index->settings->attribute_size); // total bytes / size (in bytes) of one data series
    fseek(afile, 0L, SEEK_SET);

    if (total_records2 < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", afilename, total_records2);
        exit(-1);
    }
    ////////////////////////////////////////////////////


    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    attrfile = malloc(sizeof(attribute_type)*ts_num*index->settings->attribute_size);///////////////////////////////////////////////
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    //////////////////////////////////////////////
    fread(attrfile,sizeof(attribute_type),index->settings->attribute_size*ts_num,afile);

    //////////////////////////////////////////////
    COUNT_INPUT_TIME_END
    ///////////////////////////////////
    // printf("attributes:\n");
    // for(int i = 0;i<ts_num;i++){
    //     printf(" %ld ",attrfile[i]);
    // }
    // for(int i = 0;i<ts_num*index->settings->attribute_size;i++){
    //     attrfile[i]=(attribute_type)1;
    // }//testing
    printf("attributes finished\n");
    if(total_records2 == ts_num){
        printf("NUMBER OF ATTRIBUTES == NUMBER OF TIMESERIES");
    }
    printf("TOTAL RECORDS OF ATTRIBUTES: %d , RECORDS OF TS:%d\n",total_records2,ts_num);
    ///////////////////////////////////

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas(
        index->settings->initial_fbl_buffer_size,
        pow(2, index->settings->paa_segments),
        index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    volatile unsigned long *next_iSAX_group = calloc(index->fbl->max_total_size, sizeof(unsigned long)); // EKOSMAS ADDED - 02 NOVEMBER 2020

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].next_iSAX_group = next_iSAX_group; // EKOSMAS ADDED - 02 NOVEMBER 2020
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        if (!embarrassingly_parallel)
        {
            pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas, (void *)&(input_data[i]));
        }
        else
        {
            pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas_EP, (void *)&(input_data[i]));
        }
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }

    /////////////////////////////////////
    // int attrvalue;
    // isax_node * temp = NULL;
    // int expectedleafs=0;
    // for(int i = 0; i<index->fbl->number_of_buffers; i++){
    //     if(index->fbl->soft_buffers[i].node != NULL && index->fbl->soft_buffers[i].initialized == 1){
    //         expectedleafs = index->fbl->soft_buffers[i].node->numofleafs;
    //         temp = index->fbl->soft_buffers[i].node->leftmost_leaf;
    //         //if(temp == NULL)printf("%d ",i);//root only
    //         while(temp!=NULL){              //leaf list
    //             printf(" %d_%d/%d ",i,temp->leaf_id,expectedleafs);
    //             // for(int i = 0; i<temp->buffer->partial_buffer_size;i++){
    //             //     printf(" %ld ",*temp->buffer->partial_attribute_buffer[i]);
    //             // }
    //             temp = temp->leaflist_next;
    //         }
    //     }
    // }
    // printf("\n");
    ////////////////////////////////////


    free(input_data);
    fclose(ifile);
    fclose(afile);///////////////////
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas(index, ts_num);
}
void index_creation_pRecBuf_new_ekosmas(const char *ifilename, long int ts_num, isax_index *index,const char*afilename)////////////////////////////////
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 0, NO_PARALLELISM_IN_SUBTREE,afilename);///////////////////////////////////
}

void index_creation_pRecBuf_new_geopat_func(const char *ifilename, long int ts_num, isax_index *index, char embarrassingly_parallel, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    if(chunk_size == 0){
        total_blocks = maxquerythread;
    }
    else{
        total_blocks = ts_num / chunk_size ;
    }
    long start_count = 0;
    long end_count = 0;

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;

    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        block_helper_exist[i] = 0;
    }
    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START
    COUNT_FILL_REC_BUF_TIME_END

    COUNT_CREATE_TREE_INDEX_TIME_START
    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;

        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;

    }
    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_NO_pRecBuf_worker_geopat_chunks, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_full_fai(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    unsigned long ts_per_thread = ts_num / maxquerythread;

    long start_count = 0;
    long end_count = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);


    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //-----------Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        //----------------------
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;

    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat2_FULL_FAI, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_do_all_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    unsigned long ts_per_thread = ts_num / maxquerythread;
    long start_count = 0;
    long end_count = 0;


    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }
   
    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    if(backoff == 3){
        return;
    }
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;

    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_cyclic, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_do_all(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    unsigned long ts_per_thread = ts_num / maxquerythread;
    
    long start_count = 0;
    long end_count = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }


    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
            pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat_doall, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_full_cas(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    unsigned long ts_per_thread = ts_num / maxquerythread;
    long start_count = 0;
    long end_count = 0;
 
    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }
 

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat2_FULL_CAS, (void *)&(input_data[i])); 
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_ep(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    unsigned long ts_per_thread = ts_num / maxquerythread;
    
    long start_count = 0;
    long end_count = 0;

    //Arrays for flagging the raw and isax buffers of phase1 and 2 
    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    if(backoff == 3){
        return;
    }
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;

        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;

    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
            pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat_ep, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_ep_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    if(chunk_size == 0){
        total_blocks = maxquerythread;
    }
    else{
        total_blocks = ts_num / chunk_size ;
    }
    long start_count = 0;
    long end_count = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (unsigned long  i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    block_processed_sum_buffer = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        block_processed_sum_buffer[i] = 0;
    }

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks, (void *)&(input_data[i]));

    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_ep_opt2(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    total_blocks = maxquerythread;

    long start_count = 0;
    long end_count = 0;

    //Arrays for flagging the raw and isax buffers of phase1 and 2 

    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    block_processed_sum_buffer = malloc(sizeof(unsigned char) * total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    next_ts_group_read_in_block_sum_buffer = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    block_helper_exist_sum_buffer = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        block_processed_sum_buffer[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        next_ts_group_read_in_block_sum_buffer[i].num = 0;
        block_helper_exist[i] = 0;
        block_helper_exist_sum_buffer[i] = 0;
    }
    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }


    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks_opt, (void *)&(input_data[i]));     
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_ep_opt2_chunks(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    total_blocks = ts_num / chunk_size ;

    long start_count = 0;
    long end_count = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }


    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    block_processed_sum_buffer = malloc(sizeof(unsigned char) * total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    next_ts_group_read_in_block_sum_buffer = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    block_helper_exist_sum_buffer = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        block_processed_sum_buffer[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        next_ts_group_read_in_block_sum_buffer[i].num = 0;
        block_helper_exist[i] = 0;
        block_helper_exist_sum_buffer[i] = 0;
    }

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    if(backoff == 3){
        return;
    }
    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_single_pRecBuf_worker_geopat_chunks, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
   
    total_blocks = maxquerythread;
    
    long start_count = 0;
    long end_count = 0;
    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }


    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        block_helper_exist[i] = 0;
    }



    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START
    COUNT_FILL_REC_BUF_TIME_END
    
    COUNT_CREATE_TREE_INDEX_TIME_START
    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);
    

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }
    

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_NO_pRecBuf_worker_geopat, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}


void index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer_do_all_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;
   
    total_blocks = maxquerythread;
    
    long start_count = 0;
    long end_count = 0;

   
    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START
    COUNT_FILL_REC_BUF_TIME_END

    COUNT_CREATE_TREE_INDEX_TIME_START
    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    
    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }
    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_NO_pRecBuf_worker_geopat_Do_ALL_OPT, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}



void index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer_fai(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;

    
    long start_count = 0;
    long end_count = 0;

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }
    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START
    COUNT_FILL_REC_BUF_TIME_END

    COUNT_CREATE_TREE_INDEX_TIME_START
    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);


    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;

    }
    
    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_NO_pRecBuf_worker_geopat_chunks_simple_help, (void *)&(input_data[i]));   
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}



void index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer_Chunk(const char *ifilename, long int ts_num, isax_index *index, char embarrassingly_parallel, int subtree_parallelism , unsigned long chunk_size, int helping,int backoff)
{
    /*-------- Variables for checking if buffers have finished computations---------------*/
   
    int total_blocks = 0 ;
    unsigned long ts_per_thread = ts_num / maxquerythread;

    total_blocks = ts_num / chunk_size ;
    
    long start_count = 0;
    long end_count = 0;

    int finish_raw_buffer = 0;
    int finish_receive_buffer_phase2 = 0;

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }


    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        block_helper_exist[i] = 0;
    }


    /*----------------- End Variables----------------------------------------------------*/
    // A. open input file and check its validity
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }

    // B. Read file in memory (into the "rawfile" array)
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END
    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START


    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf_geopat(
        ts_num,
        1,
        ts_num, index);


    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    pthread_t threadid[maxquerythread];                                                                         // thread's id array
    buffer_data_inmemory_ekosmas *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    unsigned long next_block_to_process_phase2 = 0; 
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    pthread_mutex_t lock_firstnode = PTHREAD_MUTEX_INITIALIZER;

    

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].lock_firstnode = &lock_firstnode; // required to initialize subtree root node during each recBuf population - not required for lock-free versions
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
        //Extra fields from geopat
        input_data[i].finish_raw_buffer = &finish_raw_buffer;
        input_data[i].subtree_parallelism = subtree_parallelism;
        input_data[i].chunk_size = chunk_size;
        input_data[i].helping = helping;
        input_data[i].num_of_helping_series_buff = 0;
        input_data[i].start_count = &start_count;
        input_data[i].end_count = &end_count;
        input_data[i].backoff = backoff;
        //phase2
        input_data[i].shared_start_number_phase2 = &next_block_to_process_phase2;
        input_data[i].finish_rec_buff_phase2 = &finish_receive_buffer_phase2;
    }
    COUNT_CREATE_TREE_INDEX_TIME_START
    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {   
        pthread_create(&(threadid[i]), NULL, index_creation_NO_pRecBuf_worker_geopat_chunks, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        num_of_helped_series += input_data[i].num_of_helping_series_buff;
        pthread_join(threadid[i], NULL);
    }
    printf("Total series helped fill receive buffer %ld \n" , num_of_helped_series);
    free(input_data);
    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    //fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END
    total_phase1_2 = fill_rec_bufs_time + create_tree_index_time;
    
    traverseRootChilds(index,current_fbl_node);

    checkTree(index,current_fbl_node);

  
    check_validity_lf_geopat(index, ts_num,subtree_parallelism);
    return;
}

void index_creation_pRecBuf_geopat_cyclic(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
    index_creation_pRecBuf_new_geopat_func_do_all_opt(ifilename, ts_num, index, subtree_parallelism,fai_step,1,0);
}

void index_creation_pRecBuf_geopat_doall(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
    index_creation_pRecBuf_new_geopat_func_do_all(ifilename, ts_num, index, subtree_parallelism,fai_step,1,0);
}

void index_creation_pRecBuf_geopat_FULL_CAS(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
    index_creation_pRecBuf_new_geopat_func_full_cas(ifilename, ts_num, index, subtree_parallelism,fai_step,1,0);
}

void index_creation_pRecBuf_geopat_fetch_and_add_phase1(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
    index_creation_pRecBuf_new_geopat_func_full_fai(ifilename, ts_num, index, subtree_parallelism,fai_step,1,0);
}
void index_creation_pRecBuf_geopat_fetch_and_add_phase1_backoff_microbenchmark(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
     index_creation_pRecBuf_new_geopat_func(ifilename, ts_num, index, 0 , subtree_parallelism,fai_step,1,2);
}

void index_initialization_rawBuffer_geopat(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism){
     index_creation_pRecBuf_new_geopat_func(ifilename, ts_num, index, 9 , subtree_parallelism,0,1,0); //Do nothing , only initializations
}
void index_creation_pRecBuf_geopat_fetch_and_add_phase1_backoff(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step,int back_off){
    index_creation_pRecBuf_new_geopat_func(ifilename, ts_num, index, 0 , subtree_parallelism,fai_step,1,back_off);
}

void index_creation_pRecBuf_geopat_fetch_and_add_phase1_caches_after_Read_file(const char *ifilename, long int ts_num, isax_index *index ,int subtree_parallelism,int fai_step){
    index_creation_pRecBuf_new_geopat_func(ifilename, ts_num, index, 0 , subtree_parallelism,fai_step,1,3);
}

//Emparassingly parallel
void index_creation_pRecBuf_geopat_ep(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism)
{
    index_creation_pRecBuf_new_geopat_func_ep(ifilename, ts_num, index, subtree_parallelism,0,1,0);
}

//EP skiping chunks
void index_creation_pRecBuf_geopat_ep_skip_chunks(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism)
{
    index_creation_pRecBuf_new_geopat_func_ep_opt(ifilename, ts_num, index, subtree_parallelism,0,1,0);
}

//Ep skipping chunks and starting from last position inside chunk
void index_creation_pRecBuf_geopat_ep_skip_chunks_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism)
{
    index_creation_pRecBuf_new_geopat_func_ep_opt2(ifilename, ts_num, index , subtree_parallelism,0,1,0);
}

void index_creation_pRecBuf_geopat_chunk_size(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism,int chunk_size)
{
    index_creation_pRecBuf_new_geopat_func_ep_opt2_chunks(ifilename, ts_num, index, subtree_parallelism,chunk_size,1,0);
}


void index_creation_No_pRecBuf_geopat_1chunk_simple_help(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism){
    index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer_fai(ifilename, ts_num, index, subtree_parallelism,1,1,0);
}

void index_creation_No_pRecBuf_geopat_do_all_opt(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism){
    index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer_do_all_opt(ifilename, ts_num, index, subtree_parallelism,1,1,0);
}

void index_creation_No_pRecBuf_geopat(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism){
    index_creation_pRecBuf_new_geopat_func_No_Receive_Buffer(ifilename, ts_num, index, subtree_parallelism,0,1,0);
}

void index_creation_No_pRecBuf_geopat_chunk_size(const char *ifilename, long int ts_num, isax_index *index, int subtree_parallelism,int chunk_size)
{
    index_creation_pRecBuf_new_geopat_func(ifilename, ts_num, index, 6 , subtree_parallelism,chunk_size,1,0);
}


void index_creation_pRecBuf_new_ekosmas_MESSI_with_enhanced_blocking_parallelism(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 0, BLOCKING_PARALLELISM_IN_SUBTREE,"");////////////////////////////// added "" cause i changed the functions signature
}
void index_creation_pRecBuf_new_ekosmas_EP(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_func(ifilename, ts_num, index, 1, NO_PARALLELISM_IN_SUBTREE,"");//////////////////////////////
}

void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    // ------------------------------------------------------------------
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    // ------------------------------------------------------------------

    // B. Read file in memory (into the "rawfile" array)
    // ------------------------------------------------------------------
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf(
        index->settings->initial_fbl_buffer_size,
        pow(2, index->settings->paa_segments),
        index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    // ------------------------------------------------------------------
    int nodeid[index->fbl->number_of_buffers];                                                                        // not used!
    pthread_t threadid[maxquerythread];                                                                               // thread's id array
    buffer_data_inmemory_ekosmas_lf *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas_lf) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute;                            // required to ensure that workers will fill in buffers only after all summaries have been computed // EKOSMAS: REMOVED 29/06/2020
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread); // EKOSMAS: REMOVED 29/06/2020

    unsigned long total_blocks = ts_num / read_block_length;
    if (read_block_length * total_blocks < ts_num)
    {
        total_blocks++;
    }

    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    // next_ts_group_read_in_block = malloc(sizeof(unsigned long)*total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    block_helpers_num = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        block_helpers_num[i] = 0;
    }

    recBuf_helpers_num = malloc(sizeof(unsigned char) * index->fbl->number_of_buffers);
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {
        recBuf_helpers_num[i] = 0;
    }

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    all_blocks_processed = 0;
    all_RecBufs_processed = 0;

    int test = 0;
    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute; // EKOSMAS: REQUIRED ONLY FOR NO_HELP VERSIONS
        input_data[i].node_counter = &node_counter;                           // required for tree construction using fai
        input_data[i].test_counter = &test; 
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }
    // ------------------------------------------------------------------

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    check_validity_ekosmas_lf(index, ts_num, parallelism_in_subtree);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, NO_PARALLELISM_IN_SUBTREE);
}

void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, BLOCKING_PARALLELISM_IN_SUBTREE);
}

void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_full_fai_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
}

void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(const char *ifilename, long int ts_num, isax_index *index, const char parallelism_in_subtree)
{
    // A. open input file and check its validity
    // ------------------------------------------------------------------
    FILE *ifile;
    ifile = fopen(ifilename, "rb");
    if (ifile == NULL)
    {
        fprintf(stderr, "File %s not found!\n", ifilename);
        exit(-1);
    }

    fseek(ifile, 0L, SEEK_END);
    file_position_type sz = (file_position_type)ftell(ifile);              // sz = size in bytes
    file_position_type total_records = sz / index->settings->ts_byte_size; // total bytes / size (in bytes) of one data series
    fseek(ifile, 0L, SEEK_SET);

    if (total_records < ts_num)
    { // check if u have the entire file
        fprintf(stderr, "File %s has only %llu records!\n", ifilename, total_records);
        exit(-1);
    }
    // ------------------------------------------------------------------

    // B. Read file in memory (into the "rawfile" array)
    // ------------------------------------------------------------------
    index->settings->raw_filename = malloc(256);
    strcpy(index->settings->raw_filename, ifilename);
    rawfile = malloc(index->settings->ts_byte_size * ts_num); // CHANGED BY EKOSMAS - 06/05/2020
    COUNT_INPUT_TIME_START
    int read_number = fread(rawfile, sizeof(ts_type), index->settings->timeseries_size * ts_num, ifile);
    COUNT_INPUT_TIME_END

    COUNT_OUTPUT_TIME_START
    COUNT_FILL_REC_BUF_TIME_START

    index->fbl = (first_buffer_layer *)initialize_pRecBuf_ekosmas_lf(
        index->settings->initial_fbl_buffer_size,
        pow(2, index->settings->paa_segments),
        index->settings->max_total_buffer_size + DISK_BUFFER_SIZE * (PROGRESS_CALCULATE_THREAD_NUMBER - 1), index);

    // C. Initialize variables and parallelize the receive buffers' fill in and index constuction
    // ------------------------------------------------------------------
    int nodeid[index->fbl->number_of_buffers];                                                                        // not used!
    pthread_t threadid[maxquerythread];                                                                               // thread's id array
    buffer_data_inmemory_ekosmas_lf *input_data = malloc(sizeof(buffer_data_inmemory_ekosmas_lf) * (maxquerythread)); // array of structs with informations we need for the workers - param for the threads - num of structs == num
    unsigned long next_block_to_process = 0;
    int node_counter = 0; // required for tree construction using fai

    pthread_barrier_t wait_summaries_to_compute; // required to ensure that workers will fill in buffers only after all summaries have been computed
    pthread_barrier_init(&wait_summaries_to_compute, NULL, maxquerythread);

    unsigned long total_blocks = ts_num / read_block_length;
    if (read_block_length * total_blocks < ts_num)
    {
        total_blocks++;
    }

    block_processed = malloc(sizeof(unsigned char) * total_blocks);
    // next_ts_group_read_in_block = malloc(sizeof(unsigned long)*total_blocks);
    next_ts_group_read_in_block = malloc(sizeof(next_ts_group) * total_blocks);
    block_helper_exist = malloc(sizeof(unsigned char) * total_blocks);
    block_helpers_num = malloc(sizeof(unsigned char) * total_blocks);
    for (int i = 0; i < total_blocks; i++)
    {
        block_processed[i] = 0;
        next_ts_group_read_in_block[i].num = 0;
        block_helper_exist[i] = 0;
        block_helpers_num[i] = 0;
    }

    recBuf_helpers_num = malloc(sizeof(unsigned char) * index->fbl->number_of_buffers);
    for (int i = 0; i < index->fbl->number_of_buffers; i++)
    {
        recBuf_helpers_num[i] = 0;
    }

    ts_processed = malloc(sizeof(unsigned char) * ts_num);
    for (int i = 0; i < ts_num; i++)
    {
        ts_processed[i] = 0;
    }

    all_blocks_processed = 0;
    all_RecBufs_processed = 0;

    for (int i = 0; i < maxquerythread; i++)
    {
        input_data[i].index = index;
        input_data[i].workernumber = i;
        input_data[i].shared_start_number = &next_block_to_process;
        input_data[i].ts_num = ts_num;
        input_data[i].wait_summaries_to_compute = &wait_summaries_to_compute;
        input_data[i].parallelism_in_subtree = parallelism_in_subtree;
        input_data[i].node_counter = &node_counter; // required for tree construction using fai
    }

    // create worker threads to fill in receive buffers (with iSAX summaries)
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_create(&(threadid[i]), NULL, index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help, (void *)&(input_data[i]));
    }

    // wait for worker threads to complete
    for (int i = 0; i < maxquerythread; i++)
    {
        pthread_join(threadid[i], NULL);
    }
    // ------------------------------------------------------------------
    /*
    for(int k = 0 ; k < index->fbl->number_of_buffers;k++){
        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[k];
        // check if the acquired receive buffer contains elements
        if (!current_fbl_node->initialized) {
            continue;
        }
        else{
            calculate_subtree_nodes(current_fbl_node->node);
        }
    }
*/

    free(input_data);
    fclose(ifile);
    COUNT_CREATE_TREE_INDEX_TIME_END
    COUNT_OUTPUT_TIME_END

    //check_validity_ekosmas_lf(index, ts_num, parallelism_in_subtree);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, NO_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_blocking_parallelism_in_subtree(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, BLOCKING_PARALLELISM_IN_SUBTREE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_announce_after_help_per_leaf(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF);
}
void index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_with_lockfree_parallelism_in_subtree_cow(const char *ifilename, long int ts_num, isax_index *index)
{
    index_creation_pRecBuf_new_ekosmas_lock_free_fai_only_after_help_func(ifilename, ts_num, index, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
}

// EKOSMAS: FUNCTION NEED FOR DEBUG - TO BE DELETED
inline unsigned long count_nodes_in_RecBuf_2(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread; k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
// EKOSMAS: FUNCTION NEED FOR DEBUG - TO BE DELETED
inline unsigned long print_nodes_in_RecBuf(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread; k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
        printf("Sub-Buffer of process [%d] contains [%d] iSAX summaries\n", k, current_fbl_node->buffer_size[k]);
    }

    return subtree_nodes;
}
inline void scan_RecBuf_for_unprocessed_iSAX_summaries(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, volatile unsigned char *stop, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree)
{
    for (int k = 0; k < maxquerythread; k++)
    {
        for (unsigned long i = 0; i < current_fbl_node->buffer_size[k] && !(*stop); i++)
        {
            if (!current_fbl_node->iSAX_processed[k][i])
            {

                // if (lockfree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE || lockfree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP) {
                //     r = malloc (sizeof(isax_node_record));
                // }

                r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
                r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;

                // Add record to index
                // if (lockfree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
                //     add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, 0);
                // }
                // else if (lockfree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP) {
                //     add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, 1);
                // }
                // else if (lockfree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF) {
                //     add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, 2);
                // }
                if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
                    lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                    lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF)
                {
                    add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);
                }
                else if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW)
                {
                    add_record_to_node_inmemory_parallel_lockfree_cow(index, current_fbl_node, r, my_id);
                }
                else
                {
                    add_record_to_node_inmemory_parallel_locks(index, current_fbl_node->node, r);
                }

                if (!current_fbl_node->iSAX_processed[k][i])
                {
                    current_fbl_node->iSAX_processed[k][i] = 1;
                }
            }
        }
    }
}
unsigned long populate_tree_lock_free_announce(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper, const char lockfree_parallelism_in_subtree)
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node)
    {
        root_node = isax_root_node_init_lockfree_announce(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, maxquerythread, lockfree_parallelism_in_subtree, current_fbl_node);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node)))
    {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    unsigned long num_added = 0;

    while (!current_fbl_node->processed)
    {
        if (lockfree_parallelism_in_subtree != LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE && !current_fbl_node->recBuf_helpers_exist)
        {
            iSAX_group = current_fbl_node->next_iSAX_group;
            current_fbl_node->next_iSAX_group = iSAX_group + 1; // EKOSMAS: ERROR: This is problematic, since the counter may return back
        }
        else
        {
            iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group), 1);
        }

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num)
        {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (iSAX_group > prev_iSAX_group_id + 1)
        { // performance enhancement
            helpers_exist = 1;
        }

        if (process_recBuf_id == maxquerythread)
        {
            break;
        }

        if (iSAX_group && iSAX_group <= prev_iSAX_group_id)
        {
            printf("\nCAUTION: populate_tree_lock_free_announce: Counter went back!!\n\n");
            fflush(stdout);
        }

        int k = process_recBuf_id;
        if (k < 0)
        {
            printf("ERRROR!!! - k equals [%d] but it can not be negative!\n", k);
            getchar();
        }
        int i = iSAX_group - prev_recBuf_iSAX_num;

        // 1st solution
        // isax_node_record *r = malloc (sizeof(isax_node_record));

        r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
        r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_lockfree_announce(index, current_fbl_node, r, my_id, maxquerythread, is_helper, lockfree_parallelism_in_subtree);

        if (!current_fbl_node->iSAX_processed[k][i])
        {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;
        prev_iSAX_group_id = iSAX_group;
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed)
    { // performance enhancement
        // if (only_after_help == 1) {
        //     scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
        // }
        // else if (only_after_help == 2) {
        //     scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP);
        // }
        // else {
        //     scan_RecBuf_for_unprocessed_iSAX_summaries (index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE);
        // }
        scan_RecBuf_for_unprocessed_iSAX_summaries(index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, lockfree_parallelism_in_subtree);
    }

    if (!current_fbl_node->processed)
    {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}

unsigned long populate_tree_lock_free_cow(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper)
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node)
    {
        root_node = isax_root_node_init_lockfree_cow(current_fbl_node->mask, index->settings->initial_leaf_buffer_size);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node)))
    {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    unsigned long num_added = 0;

    while (!current_fbl_node->processed)
    {
        iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group), 1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num)
        {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (iSAX_group > prev_iSAX_group_id + 1)
        { // performance enhancement
            helpers_exist = 1;
        }

        if (process_recBuf_id == maxquerythread)
        {
            break;
        }

        int k = process_recBuf_id;
        if (k < 0)
        {
            printf("ERRROR!!! - k equals [%d] but it can not be negative!\n", k);
            getchar();
        }
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
        r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        // printf("add record...\n"); fflush(stdout);
        add_record_to_node_inmemory_parallel_lockfree_cow(index, current_fbl_node, r, my_id);
        // printf("add record - DONE...\n"); fflush(stdout);

        if (!current_fbl_node->iSAX_processed[k][i])
        {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;
        prev_iSAX_group_id = iSAX_group;
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed)
    { // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries(index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, LOCKFREE_PARALLELISM_IN_SUBTREE_COW);
    }

    if (!current_fbl_node->processed)
    {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}

//geopat
 unsigned long populate_tree_lock_free_cow_geopat_flag(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node, isax_node_record *r, unsigned long my_id, int rec_buff_exists,unsigned long current_pos)
{   
    isax_node_single_buffer *root_node = NULL;
   
    // create and initialize a new fbl leaf node
    if(current_fbl_node->node[(unsigned long)(*r->mask)] == NULL)
    {
        root_node = isax_root_node_init_lockfree_cow_geopat((*r->mask), index->settings->initial_leaf_buffer_size,index->settings->max_leaf_size,1);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node[(unsigned long)(*r->mask)] || !CASPTR(&current_fbl_node->node[(unsigned long)(*r->mask)], NULL, root_node)))
    {
        // free memory
        isax_tree_destroy_lockfree_geopat(root_node);
    }

    root_node = current_fbl_node->node[(unsigned long)(*r->mask)];

    // populate tree
    unsigned long subtree_nodes = 0;
   
    add_record_to_node_inmemory_parallel_lockfree_geopat(index, current_fbl_node, r,my_id,*r->mask,current_pos,rec_buff_exists);


    subtree_nodes++;

    return subtree_nodes;
}



/*------------------------------- Calculating SubTreesNodes --------------------------------------------*/


 void help_subtree_path_calculation(isax_node_single_buffer * node ){
    if(node == NULL) return;


    if(node->leftSubtreeDone == 0 && node->node->is_leaf != 1){
        help_subtree_path_calculation( (isax_node_single_buffer*)node->node->left_child );
    }
    if(node->rightSubtreeDone == 0 && node->node->is_leaf != 1){
        help_subtree_path_calculation((isax_node_single_buffer*)node->node->right_child );
    }
    node->subTreeNodesNum = node->leftSubtreeCounter + node->rightSubtreeCounter;
    if(node->node->parent != NULL && (((isax_node_single_buffer*)node->node->parent)->node->left_child == node)){
        ((isax_node_single_buffer*)node->node->parent)->leftSubtreeCounter = node->subTreeNodesNum + 1;
         ((isax_node_single_buffer*)node->node->parent)->leftSubtreeDone = 1;
    }
    else if(node->node->parent != NULL && (((isax_node_single_buffer*)node->node->parent)->node->right_child == node)){
         ((isax_node_single_buffer*)node->node->parent)->rightSubtreeCounter = node->subTreeNodesNum + 1;
         ((isax_node_single_buffer*)node->node->parent)->rightSubtreeDone = 1; 
    }
    
    return;
}

 void calculate_subtree_nodes(isax_node_single_buffer * node){

    if(node == NULL) return;
    //if(node->isVisited  == 1) return;

    calculate_subtree_nodes((isax_node_single_buffer*)node->node->left_child);
    calculate_subtree_nodes((isax_node_single_buffer*)node->node->right_child);

   /* if(CASPTR(&node->isVisited,0,1)){ //problematic :(
        while(node->is_leaf != 1 && ((node->left_child != NULL && node->leftSubtreeDone == 0) || (node->right_child != NULL && node->rightSubtreeDone == 0))){
            if(node->leftSubtreeDone == 0){
                help_subtree_path_calculation(node->left_child);
            }
            else{
                help_subtree_path_calculation(node->right_child);
            }
		}
   */
        node->subTreeNodesNum = node->leftSubtreeCounter + node->rightSubtreeCounter;

        if(node->node->parent != NULL && ((isax_node_single_buffer*)node->node->parent)->node->left_child == node){
            ((isax_node_single_buffer*)node->node->parent)->leftSubtreeCounter = node->subTreeNodesNum + 1;
            ((isax_node_single_buffer*)node->node->parent)->leftSubtreeDone = 1;
        }
        else if(node->node->parent != NULL && ((isax_node_single_buffer*)node->node->parent)->node->right_child == node){
            ((isax_node_single_buffer*)node->node->parent)->rightSubtreeCounter = node->subTreeNodesNum + 1;
            ((isax_node_single_buffer*)node->node->parent)->rightSubtreeDone = 1;
        }   
   // }

    return;
}


 void traverseRootChilds(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node){
    int i = 0;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    for(i = 0 ; i < number_of_subtrees; i++){
        if(current_fbl_node->node[i] != NULL){
            calculate_subtree_nodes(current_fbl_node->node[i]);
           // printSubtree(current_fbl_node->node[i]);
        }     
    }
}


 void checkSubtree(isax_node_single_buffer *node){
    if(node != NULL){
        if(node->subTreeNodesNum != (node->leftSubtreeCounter + node->rightSubtreeCounter)){
            calculate_subtree_nodes(node);
            printf("Sum = %d , Left = %d Right = %d \n ",node->subTreeNodesNum,node->leftSubtreeCounter,node->rightSubtreeCounter );
        }
        
    }
}

void checkTree(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node){
    int i = 0;
    int number_of_subtrees = (int)pow(2, index->settings->paa_segments);
    for(i = 0 ; i < number_of_subtrees; i++){
        if(current_fbl_node->node[i] != NULL){
            checkSubtree(current_fbl_node->node[i]);
        }
    }
}




void printSubtree(isax_node *node,isax_index *index){
    if(node == NULL) {printf("NULL\t"); return;}
    printf("Left \t");
    printSubtree(node->left_child,index);
    printf("Right \t");
    printSubtree(node->right_child,index);
    if(node->is_leaf && node->buffer->partial_buffer_size > 0){    
        for(int j = 0 ; j< node->buffer->partial_buffer_size;j++){
            printf(" %d, \t",*node->buffer->partial_position_buffer[j]);
        }
        printf("\n");
    }
    return;
}




inline void printSubtree2(isax_node *node,isax_index *index){
    if(node == NULL) {printf("NULL\t"); return;}
    printf("Left \t");
    printSubtree2(node->left_child,index);
    printf("Right \t");
    printSubtree2(node->right_child,index);
    if(node->is_leaf){ 
        for(int j = 0 ; j< node->buffer->partial_buffer_size;j++){
            printf(" %d, \t",*node->buffer->partial_position_buffer[j]);
        }
        printf("\n");
    }
    return;
}

/*-------------------------------End Calculating SubTreesNodes --------------------------------------------*/



unsigned long populate_tree_with_locks(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id, const char is_helper)
{
    isax_node *root_node = NULL;

    // create and initialize a new fbl leaf node
    if (!current_fbl_node->node)
    {
        root_node = isax_root_node_init(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, NULL);
        root_node->is_leaf = 1;
        root_node->lock_node = malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(root_node->lock_node, NULL);
    }

    // try to establish root node
    if (root_node != NULL && (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node)))
    {
        // printf("ERROR!!! NOTHING SHOULD BE FREE!!!\n");fflush(stdout);
        // getchar();
        // free memory
        pthread_mutex_destroy(root_node->lock_node);
        isax_tree_destroy_lockfree(root_node);
    }

    root_node = current_fbl_node->node;

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned long prev_iSAX_group_id = 0;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;
    char helpers_exist = 0;

    while (!current_fbl_node->processed)
    {
        iSAX_group = __sync_fetch_and_add(&(current_fbl_node->next_iSAX_group), 1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num)
        {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (iSAX_group > prev_iSAX_group_id + 1)
        { // performance enhancement
            helpers_exist = 1;
        }

        if (process_recBuf_id == maxquerythread)
        {
            break;
        }

        int k = process_recBuf_id;
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
        r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_locks(index, root_node, r);

        if (!current_fbl_node->iSAX_processed[k][i])
        {
            current_fbl_node->iSAX_processed[k][i] = 1;
        }

        subtree_nodes++;
        prev_iSAX_group_id = iSAX_group;
    }

    if ((is_helper || helpers_exist) && !current_fbl_node->processed)
    { // performance enhancement
        scan_RecBuf_for_unprocessed_iSAX_summaries(index, current_fbl_node, r, &current_fbl_node->processed, my_id, is_helper, 0);
    }

    if (!current_fbl_node->processed)
    {
        current_fbl_node->processed = 1;
    }

    return subtree_nodes;
}
inline unsigned long populate_tree_copy_and_establish_lock_free(isax_index *index, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node_record *r, unsigned long my_id)
{
    // create and initialize a new fbl leaf node
    isax_node *root_node = isax_root_node_init(current_fbl_node->mask, index->settings->initial_leaf_buffer_size, current_fbl_node);
    root_node->is_leaf = 1;

    // populate tree
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !current_fbl_node->node; k++)
    {
        for (int i = 0; i < current_fbl_node->buffer_size[k] && !current_fbl_node->node; i++)
        {
            r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
            r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
            r->insertion_mode = NO_TMP | PARTIAL;
            // Add record to index
            add_record_to_node_inmemory(index, root_node, r, 1);
        }

        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    // try to establish tree copy
    if (current_fbl_node->node || !CASPTR(&current_fbl_node->node, NULL, root_node))
    {
        // free memory
        isax_tree_destroy_lockfree(root_node);
    }

    return subtree_nodes;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_copy(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, isax_node *volatile *stop)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel(parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node, volatile unsigned char *stop)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
static inline void scan_for_unprocessed_RecBufs(isax_index *index, isax_node_record *r, unsigned long my_id, const char parallelism_in_subtree)
{

    if (DO_NOT_HELP)
    {
        return;
    }

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_subtree_construction)
    {
        backoff_time *= (unsigned long)BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else
    {
        backoff_time = 0;
    }

    for (int i = 0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++)
    {

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf *)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized ||
            (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) ||
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node))
        {
            continue;
        }

        unsigned long num_nodes;

        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF)
        { //  do not backoff in this case
            ;
        }
        else if (parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE)
        {
            num_nodes = count_nodes_in_RecBuf_for_subtree_parallel(current_fbl_node, &current_fbl_node->processed);
            backoff_delay_lockfree_subtree_parallel(backoff_time * num_nodes, &current_fbl_node->processed);
        }
        else
        {
            num_nodes = count_nodes_in_RecBuf_for_subtree_copy(current_fbl_node, &current_fbl_node->node);
            backoff_delay_lockfree_subtree_copy(backoff_time * num_nodes, &current_fbl_node->node);
        }

        if ((parallelism_in_subtree != NO_PARALLELISM_IN_SUBTREE && current_fbl_node->processed) ||
            (parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE && current_fbl_node->node))
        {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;
        // __sync_fetch_and_add(&recBuf_helpers_num[i], 1);

        if (parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE)
        {
            populate_tree_with_locks(index, current_fbl_node, r, my_id, 1);
        }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE){
        //     // populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, 0);
        //     populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        // }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP){
        //     if (!current_fbl_node->recBuf_helpers_exist) {
        //         current_fbl_node->recBuf_helpers_exist = 1;
        //     }
        //     // populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, 1);
        //     populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        // }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF){
        //     // EKOSMAS: This should be moved in populate and should change per leaf
        //     // if (!current_fbl_node->recBuf_helpers_exist) {
        //     //     current_fbl_node->recBuf_helpers_exist = 1;
        //     // }

        //     // populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, 2);
        //     populate_tree_lock_free_announce (index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        // }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE)
        {
            populate_tree_lock_free_announce(index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF)
        {
            if (!current_fbl_node->recBuf_helpers_exist)
            {
                current_fbl_node->recBuf_helpers_exist = 1;
            }

            populate_tree_lock_free_announce(index, current_fbl_node, r, my_id, 1, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW)
        {
            populate_tree_lock_free_cow(index, current_fbl_node, r, my_id, 1);
        }
        else
        { // parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE
            populate_tree_copy_and_establish_lock_free(index, current_fbl_node, r, my_id);
        }
    }

    if (!all_RecBufs_processed)
    {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt)
    {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }

    if (recBufs_helped_cnt)
    {
        // printf("Thread [%d] helped [%d] receive buffers\n", my_id, recBufs_helped_cnt);
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }

    // if (backoff_time == 0) {
    //     printf("Thread [%d] processed no subtrees and helped [%d] subtrees", my_id, recBufs_helped_cnt);fflush(stdout);
    // }
}
static inline void tree_index_creation_from_pRecBuf_fai_lock_free(void *transferdata, const char parallelism_in_subtree)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf *)transferdata;
    isax_index *index = input_data->index;
    int j;

    isax_node_record *r = malloc(sizeof(isax_node_record));

    while (!all_RecBufs_processed)
    {
        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        COUNT_MY_TIME_START
        if (parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE)
        {
            my_num_subtree_nodes += populate_tree_with_locks(index, current_fbl_node, r, input_data->workernumber, 0);
        }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ){
        //     my_num_subtree_nodes += populate_tree_lock_free_announce (index, current_fbl_node, r, input_data->workernumber, 0, 0);
        // }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP){
        //     my_num_subtree_nodes += populate_tree_lock_free_announce (index, current_fbl_node, r, input_data->workernumber, 0, 1);
        // }
        // else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF){
        //     my_num_subtree_nodes += populate_tree_lock_free_announce (index, current_fbl_node, r, input_data->workernumber, 0, 2);
        // }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP ||
                 parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF)
        {
            my_num_subtree_nodes += populate_tree_lock_free_announce(index, current_fbl_node, r, input_data->workernumber, 0, parallelism_in_subtree);
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW)
        {
            my_num_subtree_nodes += populate_tree_lock_free_cow(index, current_fbl_node, r, input_data->workernumber, 0);
        }
        else
        { // parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE
            my_num_subtree_nodes += populate_tree_copy_and_establish_lock_free(index, current_fbl_node, r, input_data->workernumber);
        }
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    scan_for_unprocessed_RecBufs(index, r, input_data->workernumber, parallelism_in_subtree);

    // free(r);                                                                                     // EKOSMAS JULY 31, 2020: This is dangerous, since some helpers may still access r!
}


 inline void insert_to_tree_phase2(isax_node_record *r,unsigned long i,isax_index *index,parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,buffer_data_inmemory_ekosmas *input_data){
        r->sax = current_fbl_node->sax_records[i];  
        r->position = current_fbl_node->pos_records[i]; 
        r->mask = &current_fbl_node->masks[i];
        r->insertion_mode = NO_TMP | PARTIAL;
        populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,i);

}

inline void check_help_processing_chunk_tree_phase2(unsigned long my_ts_start,unsigned long my_ts_end,isax_node_record *r,isax_index *index,
                                                        parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,buffer_data_inmemory_ekosmas *input_data){

    for(unsigned long j = my_ts_start ; j <my_ts_end ; j++){

            if(current_fbl_node->iSAX_processed[j] == 0){
                r->sax = current_fbl_node->sax_records[j];  
                r->position = current_fbl_node->pos_records[j]; 
                r->mask = &current_fbl_node->masks[j];
                r->insertion_mode = NO_TMP | PARTIAL;
                populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,j);
            }  
    }
}

static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_doall(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;

    isax_node_record *r = malloc(sizeof(isax_node_record));

    unsigned long j = 0;

    for(j = 0 ; j < ts_num ; j++){

        if(*(current_fbl_node->finish_receive_buffer) == 1 ){
            break;
        }
        if(current_fbl_node->iSAX_processed[j] == 0){
            r->sax = current_fbl_node->sax_records[j];  
            r->position = current_fbl_node->pos_records[j]; 
            r->mask = &current_fbl_node->masks[j];
            r->insertion_mode = NO_TMP | PARTIAL;
            populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,j);
        }


    }
     *(current_fbl_node->finish_receive_buffer) = 1;  
}


static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_doallOpt(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                                    int *finish_rec_buff_phase2)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;


    isax_node_record *r = malloc(sizeof(isax_node_record));

    for(unsigned long i = my_ts_start; i < ts_num;i++){
        if(current_fbl_node->iSAX_processed[i] == 0){
            insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);
        }
    }

    for(unsigned long j = 0 ; j <my_ts_start;j++){

         if((*finish_rec_buff_phase2) == 1  ){
            break;
        }
        if(current_fbl_node->iSAX_processed[j] == 0){
            insert_to_tree_phase2(r,j,index,current_fbl_node,input_data);
        }
    }

     *finish_rec_buff_phase2 = 1;
}


static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_CAS(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;

    isax_node_record *r = malloc(sizeof(isax_node_record));

    unsigned long j = 0;
    while (1)
    {   

        j = *input_data-> shared_start_number_phase2;
        if (j >= ts_num)
        {
            break;
        }
        if(CASPTR(input_data-> shared_start_number_phase2,j,j+1)){
            r->sax = current_fbl_node->sax_records[j];  
            r->position = current_fbl_node->pos_records[j]; 
            r->mask = &current_fbl_node->masks[j];
            r->insertion_mode = NO_TMP | PARTIAL;
            populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,j);

        }
    } 

    unsigned long i = 0 ;
    //helping 
    while ( i < ts_num && maxquerythread > 1){
        if(*(current_fbl_node->finish_receive_buffer) == 1 ){
            break;
        }
        if(i < ts_num && current_fbl_node->iSAX_processed[i] == 0){
                r->sax = current_fbl_node->sax_records[i];  
                r->position = current_fbl_node->pos_records[i]; 
                r->mask = &current_fbl_node->masks[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,i);
        }
        i++;  
    } 
    *(current_fbl_node->finish_receive_buffer) = 1;
    
}





static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;

    isax_node_record *r = malloc(sizeof(isax_node_record));

    unsigned long j = 0;
    while (1)
    {   

        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= ts_num)
        {
            break;
        }
        else{
              r->sax = current_fbl_node->sax_records[j];  
              r->position = current_fbl_node->pos_records[j]; 
              r->mask = &current_fbl_node->masks[j];
              r->insertion_mode = NO_TMP | PARTIAL;
              populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,j);
        }
    } 

    unsigned long i = 0 ;
    //helping 
    while ( i < ts_num && maxquerythread > 1){
        if(*(current_fbl_node->finish_receive_buffer) == 1 ){
            break;
        }
        if(i < ts_num && current_fbl_node->iSAX_processed[i] == 0){
                r->sax = current_fbl_node->sax_records[i];  
                r->position = current_fbl_node->pos_records[i]; 
                r->mask = &current_fbl_node->masks[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 1,i);
        }
        i++;  
    } 
    *(current_fbl_node->finish_receive_buffer) = 1;
    
}



static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_basic(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                                    int *finish_rec_buff_phase2)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;
    unsigned long *next_block_to_process = input_data->shared_start_number_phase2;

    if (input_data->workernumber == maxquerythread - 1)
    { 
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }
    isax_node_record *r = malloc(sizeof(isax_node_record));

    for(unsigned long i = my_ts_start; i < my_ts_end;i++){
        insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);
    }

    unsigned long i = 0;

    while ( i < ts_num && maxquerythread > 1){
        if((*finish_rec_buff_phase2) == 1  ){
            break;
        }
        if(current_fbl_node->iSAX_processed[i] == 0){
            insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);
        }
        i++;  
    } 
     *finish_rec_buff_phase2 = 1;
}


static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_basic_skip_chunks(void *transferdata, unsigned long ts_num , 
                                                    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                                    int *finish_rec_buff_phase2)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;
    unsigned long *next_block_to_process = input_data->shared_start_number_phase2;

    if (input_data->workernumber == maxquerythread - 1)
    { 
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }
    isax_node_record *r = malloc(sizeof(isax_node_record));

    for(unsigned long i = my_ts_start; i < my_ts_end;i++){
        insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);      
    }
    block_processed_sum_buffer[input_data->workernumber] = 1;
    

    if(input_data->helping == 1 && maxquerythread > 1){ 
        for (int i = 0; i < maxquerythread; i++) // Help Unfinished Timeseries
        { 
            if((*finish_rec_buff_phase2) == 1 ){
                break;
            }
            if(block_processed_sum_buffer[i] ==  1 ){
                continue;
            }
            else{
    
               my_ts_start = ts_per_thread * i;
               my_ts_end = my_ts_start + ts_per_thread;

                if (input_data->workernumber == maxquerythread - 1)
                { 
                    my_ts_end += ts_num - ts_per_thread * maxquerythread;
                }

                for(unsigned long j = my_ts_start;j<my_ts_end;j++){
                    if((*finish_rec_buff_phase2) == 1 || block_processed_sum_buffer[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (current_fbl_node->iSAX_processed[j] == 0)
                    {
                        insert_to_tree_phase2(r,j,index,current_fbl_node,input_data);
                    } 
                }

                block_processed_sum_buffer[i] = 1; 
            }     
        }
        *finish_rec_buff_phase2 = 1;
    } 
}



static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP(void *transferdata, unsigned long ts_num , 
                                                parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                                int *finish_rec_buff_phase2)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;
    unsigned long *next_block_to_process = input_data->shared_start_number_phase2;

    if (input_data->workernumber == maxquerythread - 1)
    { 
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }
    isax_node_record *r = malloc(sizeof(isax_node_record));

   for(unsigned long i = my_ts_start; i < my_ts_end; i++){
            insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);
            if(block_helper_exist_sum_buffer[input_data->workernumber] == 0 ){
                 next_ts_group_read_in_block_sum_buffer[input_data->workernumber].num = i ;
            }
            else{
                i = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[input_data->workernumber].num,1);
            }
    }

    if(block_helper_exist_sum_buffer[input_data->workernumber] == 1) {
        check_help_processing_chunk_tree_phase2(my_ts_start,my_ts_end,r,index,current_fbl_node,input_data);
    }

    block_processed_sum_buffer[input_data->workernumber] = 1;
    //printf("Worker_number = %d\n",input_data->workernumber);
    

    if(input_data->helping == 1 && maxquerythread > 1){ 
        for (int i = 0; i < maxquerythread; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist_sum_buffer[i] = 1;
            if((*finish_rec_buff_phase2) == 1 ){
                break;
            }
            if(block_processed_sum_buffer[i] ==  1 ){
                continue;
            }
            else{
    
               my_ts_start = ts_per_thread * i;
               my_ts_end = my_ts_start + ts_per_thread;

                if (input_data->workernumber == maxquerythread - 1)
                { 
                    my_ts_end += ts_num - ts_per_thread * maxquerythread;
                }
                unsigned long j = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[i].num,1);

                while ( j < my_ts_end) {
                    if((*finish_rec_buff_phase2) == 1 || block_processed_sum_buffer[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (current_fbl_node->iSAX_processed[j] == 0)
                    {
                        insert_to_tree_phase2(r,j,index,current_fbl_node,input_data);
                    } 
                    j = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[i].num,1);
                }

                check_help_processing_chunk_tree_phase2(my_ts_start,my_ts_end,r,index,current_fbl_node,input_data);

                block_processed_sum_buffer[i] = 1; 
            }     
        }
        *finish_rec_buff_phase2 = 1;
    } 
}


static inline void tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_chunks(void *transferdata, unsigned long ts_num , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                                    int chunk_size, 
                                                    int *finish_rec_buff_phase2)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    
    unsigned long total_blocks = 0;

    total_blocks = ts_num / chunk_size;
    
    unsigned long *next_block_to_process = input_data->shared_start_number_phase2;
    unsigned long my_ts_start,my_ts_end; 
    unsigned long block_number;
     isax_node_record *r = malloc(sizeof(isax_node_record));
    while(1){

        block_number = __sync_fetch_and_add(next_block_to_process, 1)-1;
        if(block_number >= total_blocks){
            break;
        }
        my_ts_start = block_number * chunk_size;
        my_ts_end = my_ts_start + chunk_size;
        for(unsigned long i = my_ts_start; i < my_ts_end; i++){
            insert_to_tree_phase2(r,i,index,current_fbl_node,input_data);
            if(block_helper_exist_sum_buffer[block_number] == 0 ){
                 next_ts_group_read_in_block_sum_buffer[block_number].num = i ;
            }
            else{
                i = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[block_number].num,1);
            }
        }

        if(block_helper_exist[block_number] == 1){
            check_help_processing_chunk_tree_phase2(my_ts_start,my_ts_end,r,index,current_fbl_node,input_data);
        }

        block_processed_sum_buffer[block_number] = 1;
    }

    

    if(input_data->helping == 1 && maxquerythread > 1 ){ 
        for (int i = 0; i < total_blocks; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist_sum_buffer[i] = 1;
            if((*finish_rec_buff_phase2) == 1 ){
                break;
            }
            if(block_processed_sum_buffer[i] ==  1 ){
                continue;
            }
            else{
                my_ts_start = chunk_size * i;
                my_ts_end = my_ts_start + chunk_size;
                unsigned long j = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[i].num,1);
                while ( j < my_ts_end) {
                    if((*finish_rec_buff_phase2) == 1 || block_processed_sum_buffer[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (current_fbl_node->iSAX_processed[j] == 0)
                    {
                        insert_to_tree_phase2(r,j,index,current_fbl_node,input_data); 
                    } 
                    j = __sync_fetch_and_add(&next_ts_group_read_in_block_sum_buffer[i].num,1);
                } 
                check_help_processing_chunk_tree_phase2(my_ts_start,my_ts_end,r,index,current_fbl_node,input_data);
                block_processed_sum_buffer[i] = 1; 
            }     
        }
        *finish_rec_buff_phase2 = 1;

    } 
}



//////////////////////////////////////

void *
data_constr(void *data)
{
    unsigned long *a;

    a = malloc(sizeof(unsigned long));
    memcpy(a, (unsigned long *) data, sizeof(unsigned long));
    return a;
}

void
data_destr(void *data)
{
    free(data);
}

void buildLeafKdTree(isax_node* leaf,isax_index* index){
            unsigned long i;
            unsigned int npoints;
            int nthreads;
            int j, dim;
            unsigned int start, buildTime, searchTime, rectSearchTime;
            struct kd_point *pointlist;
            float *min, *max;

            npoints = leaf->buffer->partial_buffer_size;
            nthreads = 1;

            dim = index->settings->attribute_size;

            pointlist = malloc(npoints * sizeof(struct kd_point));

            min = malloc(sizeof(float)*dim);
            max = malloc(sizeof(float)*dim);

            for(int k = 0 ;k < dim;k++){
                min[k]=index->settings->attribute_min_value;
                max[k]=index->settings->attribute_max_value;
            }

            for(i = 0; i < npoints; i++) {
                pointlist[i].point = malloc(dim*sizeof(float));
                pointlist[i].data = (unsigned long *) malloc(sizeof(unsigned long));
                memcpy(pointlist[i].data, &i, sizeof(unsigned long));
                for(j = 0; j < dim; j++){
                    attribute_type*temp1=leaf->buffer->partial_attribute_buffer[i];
                    pointlist[i].point[j] = (float)temp1[j];
                }
            }


            // for(j = 0; j < dim; j++){
            //     printf("last is (%d)%d\n",j,leaf->buffer->partial_attribute_buffer[i-1][j]);
            // }


            if ((leaf->kdtree = kd_buildTree(pointlist, npoints, data_constr, data_destr,min, max, dim, nthreads)) == NULL) {
                fprintf(stderr, "Error building kd-tree\n");
                exit(EXIT_FAILURE);
            }
            for(i = 0; i < npoints; i++) {
                free(pointlist[i].data);
                free(pointlist[i].point);
            }
            free(pointlist);
}
///////////////////////////////////////



void tree_index_creation_from_pRecBuf_fai_blocking(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while (1)
    {

        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        for (int k = 0; k < maxquerythread; k++)
        {

            for (int i = 0; i < current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
                r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                //////////////////////////////////////////////
                r->attr = (attribute_type*)&(((current_fbl_node->att_records[k]))[i * index->settings->attribute_size]);
                //////////////////////////////////////////////
                r->insertion_mode = NO_TMP | PARTIAL;
                // Add record to index
                add_record_to_node_inmemory(index, (isax_node *)current_fbl_node->node, r, 1);
            }
        }
        ///////////////////////////////******************************************************************* */
        //building kd tree in each leaf and numbering leafs
        if(current_fbl_node->node->is_leaf){
            if(current_fbl_node->node->buffer->partial_buffer_size>0){buildLeafKdTree(current_fbl_node->node,index);}
                else{current_fbl_node->node->kdtree =NULL;}
        }else{
            int num=0;
            isax_node* temp = current_fbl_node->node->leftmost_leaf;
            while(temp!=NULL){
                if(temp->buffer->partial_buffer_size>0){buildLeafKdTree(temp,index);}
                    else{temp->kdtree =NULL;}
                temp->leaf_id = num++;
                temp=temp->leaflist_next;
            }
        }
    
        ///////////////////////////////******************************************************************* */
    }

    free(r);
}

inline unsigned long populate_tree_with_locks_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, int j, parallel_fbl_soft_buffer_ekosmas *current_fbl_node, isax_node_record *r, unsigned long my_id)
{
    isax_index *index = input_data->index;

    isax_node *root_node = current_fbl_node->node;
    pthread_mutex_t *tmp_lock_node = NULL;

    // create and initialize a new lock_node
    if (!root_node->lock_node)
    {
        tmp_lock_node = malloc(sizeof(pthread_mutex_t));
        pthread_mutex_init(tmp_lock_node, NULL);
    }

    // try to establish new lock_node
    if (tmp_lock_node != NULL && (root_node->lock_node || !CASPTR(&root_node->lock_node, NULL, tmp_lock_node)))
    {
        // free memory
        pthread_mutex_destroy(tmp_lock_node);
    }

    // populate tree
    unsigned long subtree_nodes = 0;
    unsigned long iSAX_group;
    unsigned int process_recBuf_id = 0;
    unsigned int prev_recBuf_iSAX_num = 0;

    while (!current_fbl_node->finished)
    {
        iSAX_group = __sync_fetch_and_add(&(input_data->next_iSAX_group[j]), 1);

        while (process_recBuf_id < maxquerythread && iSAX_group >= current_fbl_node->buffer_size[process_recBuf_id] + prev_recBuf_iSAX_num)
        {
            prev_recBuf_iSAX_num += current_fbl_node->buffer_size[process_recBuf_id];
            process_recBuf_id++;
        }

        if (process_recBuf_id == maxquerythread)
        {
            break;
        }

        int k = process_recBuf_id;
        int i = iSAX_group - prev_recBuf_iSAX_num;

        r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
        r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
        r->insertion_mode = NO_TMP | PARTIAL;

        // Add record to index
        add_record_to_node_inmemory_parallel_locks(index, root_node, r);

        subtree_nodes++;
    }

    if (!current_fbl_node->finished)
    {
        current_fbl_node->finished = 1;
    }

    return subtree_nodes;
}
inline void backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(unsigned long backoff, volatile int *stop)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff && !(*stop); i++)
        ;
}
inline unsigned long count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(parallel_fbl_soft_buffer_ekosmas *current_fbl_node, volatile int *stop)
{
    unsigned long subtree_nodes = 0;
    for (int k = 0; k < maxquerythread && !(*stop); k++)
    {
        subtree_nodes += current_fbl_node->buffer_size[k];
    }

    return subtree_nodes;
}
static inline void scan_for_unprocessed_RecBufs_blocking_with_parallelism(buffer_data_inmemory_ekosmas *input_data, isax_node_record *r, unsigned long my_id)
{

    isax_index *index = input_data->index;

    if (DO_NOT_HELP)
    {
        return;
    }

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_subtree_construction)
    {
        backoff_time *= (unsigned long)BACKOFF_SUBTREE_DELAY_PER_NODE;
    }
    else
    {
        backoff_time = 0;
    }

    for (int i = 0; i < index->fbl->number_of_buffers && !all_RecBufs_processed; i++)
    {

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[i];

        if (!current_fbl_node->initialized || current_fbl_node->finished)
        {
            continue;
        }

        unsigned long num_nodes = count_nodes_in_RecBuf_for_subtree_parallel_blocking_with_parallelism(current_fbl_node, &current_fbl_node->finished);
        backoff_delay_lockfree_subtree_parallel_blocking_with_parallelism(backoff_time * num_nodes, &current_fbl_node->finished);

        if (current_fbl_node->finished)
        {
            recBufs_helping_avoided_cnt++;
            continue;
        }

        recBufs_helped_cnt++;

        populate_tree_with_locks_blocking_with_parallelism(input_data, i, current_fbl_node, r, my_id);
    }

    if (!all_RecBufs_processed)
    {
        all_RecBufs_processed = 1;
    }

    if (recBufs_helping_avoided_cnt)
    {
        COUNT_SUBTREE_HELP_AVOIDED(recBufs_helping_avoided_cnt)
    }

    if (recBufs_helped_cnt)
    {
        COUNT_SUBTREES_HELPED(recBufs_helped_cnt)
    }
}
static inline void tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    isax_index *index = input_data->index;
    int j;
    bool has_record;
    isax_node_record *r = malloc(sizeof(isax_node_record));

    while (1)
    {
        j = __sync_fetch_and_add(input_data->node_counter, 1);
        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }

        parallel_fbl_soft_buffer_ekosmas *current_fbl_node = &((parallel_first_buffer_layer_ekosmas *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        COUNT_MY_TIME_START
        populate_tree_with_locks_blocking_with_parallelism(input_data, j, current_fbl_node, r, input_data->workernumber);
        COUNT_MY_TIME_FOR_SUBTREE_END
        my_num_subtree_construction++;
    }

    scan_for_unprocessed_RecBufs_blocking_with_parallelism(input_data, r, input_data->workernumber);

    free(r);
}

// void* index_creation_pRecBuf_worker_new_botao
void *index_creation_pRecBuf_worker_new(void *transferdata)
{

    // ADDED BY EKOSMAS - JUNE 02, 2020
    //      FROM HERE
    buffer_data_inmemory *input_data = (buffer_data_inmemory *)transferdata;
    threadPin(input_data->workernumber, maxquerythread);
    //      UP TO HERE

    sax_type *sax = malloc(sizeof(sax_type) * ((buffer_data_inmemory *)transferdata)->index->settings->paa_segments);
    // struct timeval workertimestart;
    // struct timeval writetiemstart;
    // struct timeval workercurenttime;
    // struct timeval writecurenttime;
    // double worker_total_time,tee,tss;
    // gettimeofday(&workertimestart, NULL);
    unsigned long roundfinishednumber;

    unsigned long start_number;
    unsigned long stop_number = ((buffer_data_inmemory *)transferdata)->stop_number;
    file_position_type *pos = malloc(sizeof(file_position_type));
    isax_index *index = ((buffer_data_inmemory *)transferdata)->index;
    ts_type *ts = malloc(sizeof(ts_type) * index->settings->timeseries_size);
    int paa_segments = ((buffer_data_inmemory *)transferdata)->index->settings->paa_segments;

    unsigned long i = 0;
    float *raw_file = ((buffer_data_inmemory *)transferdata)->ts;
    while (1)
    {
        start_number = __sync_fetch_and_add(((buffer_data_inmemory *)transferdata)->shared_start_number, read_block_length);
        if (start_number > stop_number)
        {
            break;
        }
        else if (start_number > stop_number - read_block_length)
        {
            roundfinishednumber = stop_number;
        }
        else
        {
            roundfinishednumber = start_number + read_block_length;
        }
        for (i = start_number; i < roundfinishednumber; i++)
        {
            // EKOSMAS: why is this memcpy required?
            memcpy(ts, &(raw_file[i * index->settings->timeseries_size]), sizeof(float) * index->settings->timeseries_size);
            if (sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
                            index->settings->paa_segments, index->settings->sax_alphabet_cardinality,
                            index->settings->sax_bit_cardinality) == SUCCESS)
            {
                *pos = (file_position_type)(i * index->settings->timeseries_size);
                memcpy(&(index->sax_cache[i * index->settings->paa_segments]), sax, sizeof(sax_type) * index->settings->paa_segments);

                isax_pRecBuf_index_insert_inmemory(index, sax, pos, ((buffer_data_inmemory *)transferdata)->lock_firstnode, ((buffer_data_inmemory *)transferdata)->workernumber, ((buffer_data_inmemory *)transferdata)->total_workernumber);
            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }
    }

    free(pos);
    free(sax);
    free(ts);
    // gettimeofday(&workercurenttime, NULL);
    // tss = workertimestart.tv_sec*1000000 + (workertimestart.tv_usec);
    // tee = workercurenttime.tv_sec*1000000  + (workercurenttime.tv_usec);
    // worker_total_time += (tee - tss);
    // printf("the worker time is %f\n",worker_total_time );

    pthread_barrier_wait(((buffer_data_inmemory *)transferdata)->lock_barrier1);
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    // pthread_barrier_wait(((buffer_data_inmemory*)transferdata)->lock_barrier2);              // REMOVED BY EKOSMAS (15/06/2020)
    bool have_record = false;
    int j;
    isax_node_record *r = malloc(sizeof(isax_node_record));
    // int preworkernumber=((buffer_data_inmemory*)transferdata)->total_workernumber;

    // for (j=((trans_fbl_input*)input)->start_number; j<((trans_fbl_input*)input)->stop_number; j++)
    while (1)
    {

        j = __sync_fetch_and_add(((buffer_data_inmemory *)transferdata)->node_counter, 1);

        if (j >= index->fbl->number_of_buffers)
        {
            break;
        }
        // fbl_soft_buffer *current_fbl_node = &index->fbl->soft_buffers[j];
        parallel_fbl_soft_buffer *current_fbl_node = &((parallel_first_buffer_layer *)(index->fbl))->soft_buffers[j];
        if (!current_fbl_node->initialized)
        {
            continue;
        }

        int i;
        have_record = false;
        for (int k = 0; k < ((buffer_data_inmemory *)transferdata)->total_workernumber; k++)
        {
            if (current_fbl_node->buffer_size[k] > 0)
                have_record = true;
            for (i = 0; i < current_fbl_node->buffer_size[k]; i++)
            {
                r->sax = (sax_type *)&(((current_fbl_node->sax_records[k]))[i * index->settings->paa_segments]);
                r->position = (file_position_type *)&((file_position_type *)(current_fbl_node->pos_records[k]))[i];
                r->insertion_mode = NO_TMP | PARTIAL;
                // Add record to index
                // printf("the position 1 is %d\n",*(r->position));
                // sleep(1);
                add_record_to_node_inmemory(index, (isax_node *)current_fbl_node->node, r, 1); // EKOSMAS: CHANGED 03 SEPTEMBER 2020
            }
        }
        if (have_record)
        {
            flush_subtree_leaf_buffers_inmemory(index, (isax_node *)current_fbl_node->node);

            // clear FBL records moved in LBL buffers

            // clear records read from files (free only prev sax buffers)
        }
    }
    free(r);
}

void *index_creation_pRecBuf_worker_new_ekosmas(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    file_position_type pos;
    sax_type *sax = malloc(sax_byte_size); // CHANGED BY EKOSMAS - 11/05/2020

    unsigned long i, block_num, my_ts_start, my_ts_end;
    while (1)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if (block_num > total_blocks)
        {
            break;
        }

        my_ts_start = block_num * read_block_length;
        if (block_num == total_blocks)
        { // there may still remain some more data series (i.e. less than #read_block_length)
            my_ts_end = ts_num;
        }
        else
        {
            my_ts_end = (block_num + 1) * read_block_length;
        }

        for (i = my_ts_start; i < my_ts_end; i++)
        {
            // EKOSMAS: why is this memcpy required?
            // EKOSMAS: TODO: check if these memcpys result in better performance
            // memcpy(ts,&(rawfile[i*index->settings->timeseries_size]), sizeof(float)*index->settings->timeseries_size);
            // if(sax_from_ts(ts, sax, index->settings->ts_values_per_paa_segment,
            if (sax_from_ts(
                    (ts_type *)&rawfile[i * index->settings->timeseries_size], // CHANGED BY EKOSMAS - 11/05/2020
                    sax,
                    index->settings->ts_values_per_paa_segment,
                    paa_segments,
                    index->settings->sax_alphabet_cardinality,
                    index->settings->sax_bit_cardinality) == SUCCESS)
            {
                pos = (file_position_type)(i * index->settings->timeseries_size);
                // memcpy(&(index->sax_cache[i*sax_byte_size]), sax, sax_byte_size);                    // REMOVED BY EKOSMAS - 04/06/2020 // CHANGED BY EKOSMAS - 11/05/2020
                                 

                isax_pRecBuf_index_insert_inmemory_ekosmas(
                    index,
                    sax,
                    &pos,                       // CHANGED BY EKOSMAS - 04/06/2020
                    input_data->lock_firstnode, // CHANGED BY EKOSMAS - 11/05/2020
                    input_data->workernumber,   // CHANGED BY EKOSMAS - 11/05/2020
                    maxquerythread);
            }
            else
            {
                fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
            }
        }
    }

    // free(pos);
    free(sax);
    // free(ts);

    pthread_barrier_wait(input_data->wait_summaries_to_compute);

    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
  
    }

    if (input_data->parallelism_in_subtree == NO_PARALLELISM_IN_SUBTREE)
        tree_index_creation_from_pRecBuf_fai_blocking(transferdata);
    else // transferdata->parallelism_in_subtree == BLOCKING_PARALLELISM_IN_SUBTREE
        tree_index_creation_from_pRecBuf_fai_blocking_with_parallelism(transferdata);


}


inline void insert_isax_recBuff(unsigned long position,isax_index *index,buffer_data_inmemory_ekosmas *input_data, sax_type *sax , file_position_type *pos,int flag){
        
        int paa_segments = index->settings->paa_segments;
        unsigned long ts_num = input_data->ts_num;
         
         if(pos == NULL && sax == NULL){
            pos = malloc(sizeof(file_position_type));
            sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
         }
          
        
    
        if (sax_from_ts(
                (ts_type *)&rawfile[position * index->settings->timeseries_size], 
                sax,
                index->settings->ts_values_per_paa_segment,
                paa_segments,
                index->settings->sax_alphabet_cardinality,
                index->settings->sax_bit_cardinality) == SUCCESS)
        {
             
            *pos = (position * index->settings->timeseries_size);
            
            isax_single_pRecBuf_index_insert_inmemory_geopat(
                index,
                sax,
                pos,                       // CHANGED BY EKOSMAS - 04/06/2020
                input_data->lock_firstnode, // CHANGED BY EKOSMAS - 11/05/2020
                input_data->workernumber,   // CHANGED BY EKOSMAS - 11/05/2020
                maxquerythread,ts_num, position,flag);
            
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
        }
     
}


inline void insert_isax_tree(parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,isax_node_record * r,unsigned long i,isax_index *index,buffer_data_inmemory_ekosmas *input_data ){
        
        int paa_segments = index->settings->paa_segments;
        r->sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
        r->position = malloc(sizeof(file_position_type));
        
        if (sax_from_ts(
                (ts_type *)&rawfile[i * index->settings->timeseries_size], 
                r->sax,
                index->settings->ts_values_per_paa_segment,
                paa_segments,
                index->settings->sax_alphabet_cardinality,
                index->settings->sax_bit_cardinality) == SUCCESS)
        {
           
            *r->position = (file_position_type)(i * index->settings->timeseries_size);
          
            root_mask_type first_bit_mask = 0x00;

            CREATE_MASK(first_bit_mask, index,r->sax);
            r->mask = &first_bit_mask;
            r->insertion_mode = NO_TMP | PARTIAL;

            populate_tree_lock_free_cow_geopat_flag(index, current_fbl_node, r, input_data->workernumber, 0,i);           
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                    failed to be created");
        }

}


inline void check_help_processing_chunk(unsigned long my_ts_start, unsigned long my_ts_end,isax_index *index,buffer_data_inmemory_ekosmas *input_data,int memcpy){
    
    int paa_segments = index->settings->paa_segments;
    unsigned long  ts_num = input_data->ts_num;
    
    for(unsigned long i = my_ts_start; i < my_ts_end;i++){
        if(ts_processed[i]  == 0){
            file_position_type *pos = malloc(sizeof(file_position_type));
            sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments); 

            if (sax_from_ts(
                    (ts_type *)&rawfile[i * index->settings->timeseries_size], 
                    sax,
                    index->settings->ts_values_per_paa_segment,
                    paa_segments,
                    index->settings->sax_alphabet_cardinality,
                    index->settings->sax_bit_cardinality) == SUCCESS)
                    {
                        *pos = (i * index->settings->timeseries_size);


                        isax_single_pRecBuf_index_insert_inmemory_geopat(
                        index,
                        sax,
                        pos,                       // CHANGED BY EKOSMAS - 04/06/2020
                        input_data->lock_firstnode, // CHANGED BY EKOSMAS - 11/05/2020
                        input_data->workernumber,   // CHANGED BY EKOSMAS - 11/05/2020
                        maxquerythread,ts_num, i,memcpy);
                        ts_processed[i] = 1;
                    }
                    else
                    {
                        fprintf(stderr, "error: cannot insert record in index, since sax representation\
                                failed to be created");
                    }
        }
    }   
}


void check_help_processing_chunk_tree(unsigned long my_ts_start,unsigned long my_ts_end,parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                        isax_index *index, buffer_data_inmemory_ekosmas *input_data){
    
    int paa_segments = index->settings->paa_segments;
    isax_node_record * r = malloc(sizeof(isax_node_record));
    for(unsigned long i = my_ts_start;i<my_ts_end;i++){
            if(ts_processed[i] == 0){
              insert_isax_tree(current_fbl_node,r,i,index,input_data);
              input_data->num_of_helping_series_buff++;
              ts_processed[i] = 1;
            }

        }

}

void *index_creation_single_pRecBuf_worker_geopat_ep(void *transferdata)
{
    
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;


    
    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }

    unsigned long i;
    //file_position_type *pos = malloc(sizeof(file_position_type));
    //sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    for (unsigned long i = my_ts_start; i < my_ts_end; i++)
    {
        insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
        ts_processed[i] = 1;
    }    

    //If helping is enabled

    if(input_data->helping == 1){
        if(CASPTR(input_data->start_count,0,1)){
            COUNT_HELP_REC_BUFFER
        }
         for (unsigned long i = 0; i < ts_num; i++) // Help Unfinished Timeseries
         { 
            if(*(input_data->finish_raw_buffer) == 1){
                break;
            }
            if (ts_processed[i] != 1)
            {
                insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
                input_data->num_of_helping_series_buff++;
            } 
        }
               //If helping is enabled
        if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *(input_data->finish_raw_buffer) = 1;
    }

   
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        
    }
   
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    
    COUNT_CREATE_TREE_INDEX_TIME_START

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_basic(transferdata ,ts_num , current_fbl_node,input_data->finish_rec_buff_phase2 );
    
       
}

 void backoff_delay(unsigned long backoff)
{
    if (!backoff)
    {
        return;
    }

    volatile unsigned long i;

    for (i = 0; i < backoff ; i++){;}
        
}

void *index_creation_single_pRecBuf_worker_geopat_doall(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;

    unsigned long i;

    file_position_type *pos = malloc(sizeof(file_position_type));
    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    for (unsigned long i = 0; i< ts_num; i++)
    {

        if((*input_data->finish_raw_buffer) == 1 ){ //if someone else finishes the buffer break.
                    break;
        }
        if(ts_processed[i] != 1){
            insert_isax_recBuff(i,index,input_data,sax,pos,1);
            ts_processed[i] = 1;
        }
        
    }

    *(input_data->finish_raw_buffer) = 1;
      
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }  
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_doall(transferdata ,ts_num , current_fbl_node );
    

    
    
}

void *index_creation_single_pRecBuf_worker_cyclic(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;


    unsigned long i;
    int found = 0 ; 
    //file_position_type *pos = malloc(sizeof(file_position_type));
    //sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    for (unsigned long i = my_ts_start; i< ts_num; i++)
    {

        if((*input_data->finish_raw_buffer) == 1 ){ //if someone else finishes the buffer break.
                    break;
        }
        if(ts_processed[i] != 1){
            insert_isax_recBuff(i,index,input_data,NULL,NULL,1);
            ts_processed[i] = 1;
        }
        
    }

    for(unsigned long i = 0 ; i < my_ts_start; i++){

        if((*input_data->finish_raw_buffer) == 1 ){ //if someone else finishes the buffer break.
                    break;
        }

        if(ts_processed[i] != 1){
            insert_isax_recBuff(i,index,input_data,NULL,NULL,1);
            ts_processed[i] = 1;
        }
    }

    *(input_data->finish_raw_buffer) = 1;


        
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }  
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_doallOpt(transferdata ,ts_num , current_fbl_node,input_data->finish_rec_buff_phase2 );
    

    
}

void *index_creation_single_pRecBuf_worker_geopat2_FULL_FAI(void *transferdata)
{
   buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    
    unsigned long total_blocks = ts_num / input_data->chunk_size;
    isax_index *index = input_data->index;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;

    unsigned long i = 0, time_series_num = 0, my_ts_start = 0, my_ts_end = 0;
    unsigned long block_number = 0;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long  count = 0 ;
    //COUNT_PHASE1_TIME_PER_THREAD(input_data->workernumber) 
    int backoff_benchmark = input_data->backoff;


    
    while (1)
    {
        block_number = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_number >= total_blocks){
             break;
        }
        my_ts_start = block_number * input_data->chunk_size;
        my_ts_end = my_ts_start + input_data->chunk_size;
        for(unsigned long i = my_ts_start; i < my_ts_end;i++){   

            insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
            ts_processed[i] = 1;
        }
        if(backoff_benchmark != 0){
            backoff_delay(rand()%1024);
        }
        count++;      
    }

    
    
    if(input_data->helping == 1){
        if(CASPTR(input_data->start_count,0,1)){
            COUNT_HELP_REC_BUFFER
        }
        for (unsigned long i = 0; i < ts_num; i++) // Help Unfinished Timeseries
        { 
           
            if(*(input_data->finish_raw_buffer) == 1){
                break;
            }
            if (ts_processed[i] != 1)
            {
                insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
                input_data->num_of_helping_series_buff++;
                ts_processed[i] = 1;
            } 
        }
        if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }   
         *(input_data->finish_raw_buffer) = 1;   
    }
    
   
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    if (input_data->workernumber == 0)
    {
        unsigned long count = 0 ;
        COUNT_FILL_REC_BUF_TIME_END
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag(transferdata ,ts_num , current_fbl_node );

   
    
}

void *index_creation_single_pRecBuf_worker_geopat2_FULL_CAS(void *transferdata)
{
   buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    
    unsigned long total_blocks = ts_num / input_data->chunk_size;
    isax_index *index = input_data->index;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;

    unsigned long i = 0, time_series_num = 0;
    unsigned long block_number = 0;

    //file_position_type *pos = malloc(sizeof(file_position_type));
    //sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    while (1)
    {  
        block_number = *input_data->shared_start_number;

        if(block_number >= ts_num){
            break;
        }

        if(CASPTR(input_data->shared_start_number,block_number,block_number+1)){
             insert_isax_recBuff(block_number,index,input_data,NULL,NULL,0);
             ts_processed[i] = 1;
        }
    }

    

    
    if(input_data->helping == 1){
        if(CASPTR(input_data->start_count,0,1)){
            COUNT_HELP_REC_BUFFER
        }
        for (unsigned long i = 0; i < ts_num; i++) // Help Unfinished Timeseries
        { 
           
            if(*(input_data->finish_raw_buffer) == 1){
                break;
            }
            if (ts_processed[i] != 1)
            {
                insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
                input_data->num_of_helping_series_buff++;
            } 
        }
        if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }   
         *(input_data->finish_raw_buffer) = 1;   
    }
    
   
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    if (input_data->workernumber == 0)
    {
        unsigned long count = 0 ;
        COUNT_FILL_REC_BUF_TIME_END
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    


    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_CAS(transferdata ,ts_num , current_fbl_node );
    
   
}


void *index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    //file_position_type *pos = malloc(sizeof(file_position_type));
    //sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }

    unsigned long i;
    for (unsigned long i = my_ts_start; i < my_ts_end; i++)
    {
        insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
        ts_processed[i] = 1;
    }

    block_processed[input_data->workernumber] = 1;


    if(input_data->helping == 1){
        if(CASPTR(input_data->start_count,0,1)){
            COUNT_HELP_REC_BUFFER
        }
        for (int i = 0; i < maxquerythread; i++) // Help Unfinished Timeseries
        { 
            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if(block_processed[i] ==  1 ){
                continue;
            }
            else{
                ts_per_thread = ts_num / maxquerythread;
                my_ts_start = ts_per_thread * i;
                my_ts_end = my_ts_start + ts_per_thread;

                if (i == (maxquerythread - 1))
                { // there may still remain some more data series (i.e. less than #maxquerythread)
                    my_ts_end += ts_num - ts_per_thread * maxquerythread;
                }

                for(unsigned long j = my_ts_start; j < my_ts_end ; j++){

                    if((*input_data->finish_raw_buffer) == 1 || block_processed[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (ts_processed[j] != 1)
                    {
                       insert_isax_recBuff(j,index,input_data,NULL,NULL,0);
                        input_data->num_of_helping_series_buff++;
                        ts_processed[j] = 1;
                    } 
                }

              //  check_help_processing_chunk(my_ts_start,my_ts_end,index,input_data);
                
                block_processed[i] = 1; 
            }

            
        }
        if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *(input_data->finish_raw_buffer) = 1;
    }

   
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }  
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_basic_skip_chunks(transferdata ,ts_num , current_fbl_node,input_data->finish_rec_buff_phase2);
    

    
    
}


void *index_creation_single_pRecBuf_worker_geopat_ep_skip_chunks_opt(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;
    
    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }
    unsigned long i = my_ts_start;
   // file_position_type *pos = malloc(sizeof(file_position_type)); 
   // sax_type *sax =  malloc(sizeof(sax_type) * index->settings->paa_segments);

    while(i < my_ts_end){
        insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
        ts_processed[i] = 1;
        if(block_helper_exist[input_data->workernumber]){ // Helper inside this chunk exists so fetch and increment
            i = __sync_fetch_and_add(&next_ts_group_read_in_block[input_data->workernumber].num,1);
        }
        else{
             i++;
             next_ts_group_read_in_block[input_data->workernumber].num = i ;
        }
    }
    if(block_helper_exist[input_data->workernumber]){
        check_help_processing_chunk(my_ts_start,my_ts_end,index,input_data,0);
    }
    
   block_processed[input_data->workernumber] = 1;


    if(input_data->helping == 1 && maxquerythread > 1){
       if(CASPTR(input_data->start_count,0,1)){
            printf("Starting\n");
            COUNT_HELP_REC_BUFFER
        }   
       
        for (int i = 0; i < maxquerythread; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist[i] = 1;
            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if(block_processed[i] ==  1 ){
                continue;
            }
            else{
                ts_per_thread = ts_num / maxquerythread;
                my_ts_start = ts_per_thread * i;
                my_ts_end = my_ts_start + ts_per_thread;

                if (i == (maxquerythread - 1))
                { // there may still remain some more data series (i.e. less than #maxquerythread)
                    my_ts_end += ts_num - ts_per_thread * maxquerythread;
                }
                unsigned long j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                while(j < my_ts_end ){
                    if((*input_data->finish_raw_buffer) == 1 || block_processed[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (ts_processed[j] != 1)
                    {   
                        insert_isax_recBuff(j,index,input_data,NULL,NULL,0);
                        input_data->num_of_helping_series_buff++;
                        ts_processed[j] = 1;
                    } 

                      j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                }
                check_help_processing_chunk(my_ts_start,my_ts_end,index,input_data,0);
                block_processed[i] = 1;
            }      
        }  
        if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
            printf("Ending\n");
        }
        *input_data->finish_raw_buffer = 1;
    }    

   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {   COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        unsigned long size = 0;
  
        printf("Checking Buffer, size = %d \n",ts_num);
        unsigned long masks_created = 0;
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
                if( *current_fbl_node->pos_records[i] > ts_num * 256){
                    printf("ERROR WRONG POSITION %ld at %ld \n",*current_fbl_node->pos_records[i] ,i);
                }
               count++;
            }
            else{
                printf("Error at buffer check at position %ld \n",i);
                exit(0);
            } 
        }     
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }  
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP(transferdata ,ts_num , current_fbl_node,input_data->finish_rec_buff_phase2);
    

    
    
}



void *index_creation_single_pRecBuf_worker_geopat_chunks(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / input_data->chunk_size;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    
    unsigned long my_ts_start,my_ts_end; 
    unsigned long block_number;

   // file_position_type *pos = malloc(sizeof(file_position_type));
   // sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);

    while(1){

        block_number = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_number >= total_blocks){
            break;
        }
        my_ts_start = block_number * input_data->chunk_size;
        my_ts_end = my_ts_start + input_data->chunk_size;
        for(unsigned long i = my_ts_start; i < my_ts_end;){

            insert_isax_recBuff(i,index,input_data,NULL,NULL,0);
            ts_processed[i] = 1;
            if(block_helper_exist[block_number]){ // Helper inside this chunk exists so fetch and increment
                i = __sync_fetch_and_add(&next_ts_group_read_in_block[block_number].num,1);
            }
            else{
                i++;
                next_ts_group_read_in_block[block_number].num = i ;
            }
        }
        if(block_helper_exist[block_number] == 1){
            check_help_processing_chunk(my_ts_start,my_ts_end,index,input_data,0);
        }
        block_processed[block_number] = 1;
    }

    

    if(input_data->helping == 1 && maxquerythread > 1){
        if(CASPTR(input_data->start_count,0,1)){
            printf("Helping Started\n");
            COUNT_HELP_REC_BUFFER
        }   
    
        for (unsigned long i = 0; i < total_blocks; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist[i] = 1;

            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if(block_processed[i] ==  1 ){
                continue;
            }
            else{
                
                my_ts_start = input_data->chunk_size * i;
                my_ts_end = my_ts_start + input_data->chunk_size;
                
                unsigned long j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                while(j < my_ts_end ){
                    if((*input_data->finish_raw_buffer) == 1 || block_processed[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (ts_processed[j] != 1)
                    {
                        insert_isax_recBuff(j,index,input_data,NULL,NULL,0);
                        input_data->num_of_helping_series_buff++;
                        ts_processed[j] = 1;
                    } 

                      j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                }
                check_help_processing_chunk(my_ts_start,my_ts_end,index,input_data,0);
                block_processed[i] = 1; 
            }
        }          
        if(CASPTR(input_data->end_count,0,1)){
            printf("Helping Ended\n");
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *input_data->finish_raw_buffer = 1;
    }
    
    
   
   parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
   parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
   
    if (input_data->workernumber == 0)
    {   COUNT_FILL_REC_BUF_TIME_END
        unsigned long count = 0 ;   
        printf("Checking Buffer, size = %d \n",ts_num);
        for(unsigned long  i = 0 ; i <ts_num;i++){
            if(current_fbl_node->pos_records[i] != NULL && current_fbl_node->sax_records[i] != NULL){
               count++;
            }
            else{
                printf("Error at buffer check \n");
                exit(0);
            } 
        }
        if(count != ts_num){
            printf("Error buffer not complete filled. Expected %ld != %ld\n",ts_num,count);
            exit(0);
        }
        else{
            printf("Buffer is completed Correctly %d\n",ts_num);
        }
        COUNT_CREATE_TREE_INDEX_TIME_START
    }  
    pthread_barrier_wait(input_data->wait_summaries_to_compute);
    

    tree_index_creation_from_pRecBuf_fai_lock_free_geopat_flag_EP_chunks(transferdata ,ts_num , current_fbl_node ,input_data->chunk_size,input_data->finish_rec_buff_phase2);

}

void *index_creation_NO_pRecBuf_worker_geopat_Do_ALL_OPT(void *transferdata)
{
  
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    threadPin(input_data->workernumber, maxquerythread);

    if(input_data->workernumber == 0 ){
        COUNT_FILL_REC_BUF_TIME_END
    }
    

    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }

    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    isax_node_record * r = malloc(sizeof(isax_node_record));

    for (unsigned long i = my_ts_start; i < ts_num; i++)
    {
        if (ts_processed[i] == 0){
            insert_isax_tree(current_fbl_node,r,i,index,input_data);  
            ts_processed[i] = 1; 
        }  
    }
    for (unsigned long i = 0; i < my_ts_start; i++)
    {
        if (ts_processed[i] == 0){
            insert_isax_tree(current_fbl_node,r,i,index,input_data);
            input_data->num_of_helping_series_buff++;  
            ts_processed[i] = 1; 
        }  
    }
    
}

void *index_creation_NO_pRecBuf_worker_geopat(void *transferdata)
{
  
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;
    threadPin(input_data->workernumber, maxquerythread);

    if(input_data->workernumber == 0 ){
        COUNT_FILL_REC_BUF_TIME_END
    }
    

    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / read_block_length;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    
    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }

    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    isax_node_record * r = malloc(sizeof(isax_node_record));

    for (unsigned long i = my_ts_start; i < my_ts_end; i++)
    {
        
        insert_isax_tree(current_fbl_node,r,i,index,input_data);
        ts_processed[i] = 1;
        if(block_helper_exist[input_data->workernumber] == 1){ // Helper inside this chunk exists so fetch and increment
            i = __sync_fetch_and_add(&next_ts_group_read_in_block[input_data->workernumber].num,1);
        }
        else{
             next_ts_group_read_in_block[input_data->workernumber].num = i+1 ;
        }      
    }

    if(block_helper_exist[input_data->workernumber]){
        check_help_processing_chunk_tree(my_ts_start,my_ts_end,current_fbl_node,index,input_data);
    }

    block_processed[input_data->workernumber] = 1;    
   
   
    if(input_data->helping == 1){
        if(CASPTR(input_data->start_count,0,1)){
            printf("Helping started\n");
            COUNT_HELP_REC_BUFFER
        }   
        for (int i = 0; i < maxquerythread; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist[i] = 1;
            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if(block_processed[i] ==  1 ){
                continue;
            }
            else{
                ts_per_thread = ts_num / maxquerythread;
                my_ts_start = ts_per_thread * i;
                my_ts_end = my_ts_start + ts_per_thread;

                if (i == (maxquerythread - 1))
                { // there may still remain some more data series (i.e. less than #maxquerythread)
                    my_ts_end += ts_num - ts_per_thread * maxquerythread;
                }
                while(1){
                   
                    if((*input_data->finish_raw_buffer) == 1 || block_processed[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }

                    unsigned long j =  __sync_fetch_and_add(&next_ts_group_read_in_block[i].num, 1);

                   if(j >= my_ts_end){
                       break;
                   }
                    if (ts_processed[j] == 0)
                    {
                        insert_isax_tree(current_fbl_node,r,j,index,input_data);
                        input_data->num_of_helping_series_buff++;
                        ts_processed[j] = 1;
                    } 
                }
                check_help_processing_chunk_tree(my_ts_start,my_ts_end,current_fbl_node,index,input_data);          
                block_processed[i] = 1; 
            }

        }
         if(CASPTR(input_data->end_count,0,1)){
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *(input_data->finish_raw_buffer) = 1;
                   
    }  
      
       
}

void *index_creation_NO_pRecBuf_worker_geopat_chunks_simple_help(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / input_data->chunk_size;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    
    unsigned long my_ts_start,my_ts_end; 
    unsigned long block_number;

    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
    isax_node_record * r = malloc(sizeof(isax_node_record));

    while(1){

        block_number = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_number >= total_blocks){
            break;
        }
        my_ts_start = block_number * input_data->chunk_size;
        my_ts_end = my_ts_start + input_data->chunk_size;
        for(unsigned long i = my_ts_start; i < my_ts_end;i++){    
            insert_isax_tree(current_fbl_node,r,i,index,input_data);
            ts_processed[i] = 1;
        }
    }

    

    if(input_data->helping == 1 && maxquerythread > 1){
        if(CASPTR(input_data->start_count,0,1)){
            printf("Helping Started\n");
            COUNT_HELP_REC_BUFFER
        }   
    
        for (unsigned long i = 0; i < ts_num; i++) // Help Unfinished Timeseries
        { 

            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if (ts_processed[i] == 0)
            {
                insert_isax_tree(current_fbl_node,r,i,index,input_data);
                input_data->num_of_helping_series_buff++;
                ts_processed[i] = 1;
            } 
              
        }
                  
        if(CASPTR(input_data->end_count,0,1)){
            printf("Helping Ended\n");
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *input_data->finish_raw_buffer = 1;
    }
    
}

void *index_creation_NO_pRecBuf_worker_geopat_chunks(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);
    unsigned long ts_num = input_data->ts_num;
    unsigned long total_blocks = ts_num / input_data->chunk_size;

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    
    unsigned long my_ts_start,my_ts_end; 
    unsigned long block_number;

    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);
    isax_node_record * r = malloc(sizeof(isax_node_record));

    while(1){

        block_number = __sync_fetch_and_add(next_block_to_process, 1);
        if(block_number >= total_blocks){
            break;
        }
        my_ts_start = block_number * input_data->chunk_size;
        my_ts_end = my_ts_start + input_data->chunk_size;
        for(unsigned long i = my_ts_start; i < my_ts_end;){
            
            insert_isax_tree(current_fbl_node,r,i,index,input_data);
            ts_processed[i] = 1;
            if(block_helper_exist[block_number]){ // Helper inside this chunk exists so fetch and increment
                i = __sync_fetch_and_add(&next_ts_group_read_in_block[block_number].num,1);
            }
            else{
                i++;
                next_ts_group_read_in_block[block_number].num = i ;
            }
        }
        if(block_helper_exist[block_number]){
            check_help_processing_chunk_tree(my_ts_start,my_ts_end,current_fbl_node,index,input_data);
        }
        block_processed[block_number] = 1;
    }

    

    if(input_data->helping == 1 && maxquerythread > 1){
        if(CASPTR(input_data->start_count,0,1)){
            printf("Helping Started\n");
            COUNT_HELP_REC_BUFFER
        }   
    
        for (unsigned long i = 0; i < total_blocks; i++) // Help Unfinished Timeseries
        { 
            block_helper_exist[i] = 1;

            if((*input_data->finish_raw_buffer) == 1 ){
                break;
            }
            if(block_processed[i] ==  1 ){
                continue;
            }
            else{
                
                my_ts_start = input_data->chunk_size * i;
                my_ts_end = my_ts_start + input_data->chunk_size;
                
                unsigned long j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                while(j < my_ts_end ){
                    if((*input_data->finish_raw_buffer) == 1 || block_processed[i] ==  1){ //if someone else finishes the buffer break.
                            break;
                    }
                    if (ts_processed[j] == 0)
                    {
                        insert_isax_tree(current_fbl_node,r,j,index,input_data);
                        input_data->num_of_helping_series_buff++;
                        ts_processed[j] = 1;
                    } 

                      j = __sync_fetch_and_add(&next_ts_group_read_in_block[i].num,1); 
                }
                check_help_processing_chunk_tree(my_ts_start,my_ts_end,current_fbl_node,index,input_data);
                block_processed[i] = 1; 
            }
        }          
        if(CASPTR(input_data->end_count,0,1)){
            printf("Helping Ended\n");
            COUNT_HELP_FILL_REC_BUF_TIME_END
        }
        *input_data->finish_raw_buffer = 1;
    }
    
}







// Embarrassingly Parallel
void *index_creation_pRecBuf_worker_new_ekosmas_EP(void *transferdata)
{
    buffer_data_inmemory_ekosmas *input_data = (buffer_data_inmemory_ekosmas *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long ts_num = input_data->ts_num;
    // unsigned long total_blocks = ts_num/read_block_length;

    isax_index *index = input_data->index;
    // unsigned long *next_block_to_process = input_data->shared_start_number;

    int paa_segments = index->settings->paa_segments;
    int sax_byte_size = index->settings->sax_byte_size;
    file_position_type pos;
    sax_type *sax = malloc(sax_byte_size);

    unsigned long ts_per_thread = ts_num / maxquerythread;
    unsigned long my_ts_start = ts_per_thread * input_data->workernumber;
    unsigned long my_ts_end = my_ts_start + ts_per_thread;

    if (input_data->workernumber == maxquerythread - 1)
    { // there may still remain some more data series (i.e. less than #maxquerythread)
        my_ts_end += ts_num - ts_per_thread * maxquerythread;
    }

    for (unsigned long i = my_ts_start; i < my_ts_end; i++)
    {
        if (sax_from_ts(
                (ts_type *)&rawfile[i * index->settings->timeseries_size],
                sax,
                index->settings->ts_values_per_paa_segment,
                paa_segments,
                index->settings->sax_alphabet_cardinality,
                index->settings->sax_bit_cardinality) == SUCCESS)
        {
            pos = (file_position_type)(i * index->settings->timeseries_size);

            isax_pRecBuf_index_insert_inmemory_ekosmas(
                index,
                sax,
                &pos,
                input_data->lock_firstnode,
                input_data->workernumber,
                maxquerythread);
        }
        else
        {
            fprintf(stderr, "error: cannot insert record in index, since sax representation\
                failed to be created");
        }
    }

    free(sax);

    pthread_barrier_wait(input_data->wait_summaries_to_compute);

    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    tree_index_creation_from_pRecBuf_fai_blocking(transferdata);
}

inline void store_isax_in_pRecBuf(buffer_data_inmemory_ekosmas_lf *input_data, isax_index *index, unsigned long ts_id)
{
    file_position_type pos;
    int sax_byte_size = index->settings->sax_byte_size;
    int paa_segments = index->settings->paa_segments;

    sax_type *sax = malloc(sax_byte_size);

    if (sax_from_ts(
            (ts_type *)&rawfile[ts_id * index->settings->timeseries_size],
            sax,
            index->settings->ts_values_per_paa_segment,
            paa_segments,
            index->settings->sax_alphabet_cardinality,
            index->settings->sax_bit_cardinality) == SUCCESS)
    {
        pos = (file_position_type)(ts_id * index->settings->timeseries_size);

        // isax_pRecBuf_index_insert_inmemory(
        //             index,
        //             sax,
        //             &pos,
        //             input_data->lock_firstnode,
        //             input_data->workernumber,
        //             maxquerythread);

        // Create mask for the first bit of the sax representation
        root_mask_type first_bit_mask = 0x00;
        CREATE_MASK(first_bit_mask, index, sax);

        insert_to_pRecBuf_lock_free(
            (parallel_first_buffer_layer_ekosmas_lf *)(index->fbl),
            sax,
            &pos,
            first_bit_mask,
            index,
            input_data->workernumber,
            maxquerythread,
            input_data->parallelism_in_subtree);
    }
    else
    {
        fprintf(stderr, "error: cannot insert record in index, since sax representation failed to be created");
    }
}
static inline void scan_block_for_unprocessed_ts(buffer_data_inmemory_ekosmas_lf *input_data, isax_index *index, unsigned long my_ts_start, unsigned long my_ts_end, volatile unsigned char *stop)
{
    for (unsigned long ts_id = my_ts_start; ts_id < my_ts_end && !(*stop); ts_id++)
    {
        if (!ts_processed[ts_id])
        {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            if (!ts_processed[ts_id])
            {
                ts_processed[ts_id] = 1;
            }
        }
    }
}

// EKOSMAS: FUNCTION NEEDED ONLY FOR TESTING - TO BE DELETED
static void process_block_minimal(unsigned long block_num, unsigned long total_blocks, unsigned long total_ts_num, buffer_data_inmemory_ekosmas_lf *input_data, isax_index *index, char is_helper, char fai_only_after_help)
{
    unsigned long my_ts_start, my_ts_end, ts_id;

    my_ts_start = block_num * read_block_length;
    if (block_num == total_blocks - 1)
    { // there may still remain some more data series (i.e. less than #read_block_length)
        my_ts_end = total_ts_num;
    }
    else
    {
        my_ts_end = my_ts_start + read_block_length;
    }

    unsigned long total_groups_in_block = (my_ts_end - my_ts_start) / ts_group_length;
    if (total_groups_in_block * ts_group_length < my_ts_end - my_ts_start)
    {
        total_groups_in_block++;
    }

    unsigned long prev_group_id = 0;
    char helpers_exist = 0;

    if (!is_helper)
    {
        COUNT_MY_TIME_START
    }

    while (!block_processed[block_num])
    {
        unsigned long ts_group;

        // if (fai_only_after_help && !block_helper_exist[block_num]) {
        ts_group = next_ts_group_read_in_block[block_num].num;
        // next_ts_group_read_in_block[block_num].num++;                       // EKOSMAS: ERROR: This is problematic, since a ts_group may be lost
        next_ts_group_read_in_block[block_num].num = ts_group + 1; // EKOSMAS: ERROR: This is again problematic, since the counter may return back
        // }
        // else {
        //     ts_group = __sync_fetch_and_add(&next_ts_group_read_in_block[block_num].num, 1);
        // }

        if (ts_group > prev_group_id + 1)
        {                      // performance enhancement
            helpers_exist = 1; // EKOSMAS: helpers_exist can be replaced with block_helper_exist[block_num], after changing the corresponding line during scan_for_unprocessed_blocks in order to set this bit all the time and not only when fai_only_after_help!=0
        }

        unsigned long ts_group_start = my_ts_start + ts_group * ts_group_length;
        unsigned long ts_group_end;

        if (ts_group >= total_groups_in_block)
        {
            break;
        }

        if (ts_group == total_groups_in_block - 1)
        {
            ts_group_end = my_ts_end;
        }
        else
        {
            ts_group_end = ts_group_start + ts_group_length;
        }

        if (ts_group && ts_group <= prev_group_id)
        {
            printf("\nCAUTION: Counter went back!!\n\n");
            fflush(stdout);
        }

        for (unsigned long ts_id = ts_group_start; ts_id < ts_group_end; ts_id++)
        {
            // if (block_processed[block_num]){
            //     return;
            // }

            // if (!ts_processed[ts_id]) {
            store_isax_in_pRecBuf(input_data, index, ts_id);
            //     if (!ts_processed[ts_id]) {
            //         ts_processed[ts_id] = 1;
            //     }
            // }
        }

        prev_group_id = ts_group; // performance enhancement
    }

    if ((is_helper || helpers_exist) && !block_processed[block_num])
    { // performance enhancement
        printf("!!!! ERROR: Scanning for unprocessed ts!!! WHY??????\n");
        fflush(stdout);
        scan_block_for_unprocessed_ts(input_data, index, my_ts_start, my_ts_end, &block_processed[block_num]);
    }
    else
    {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;
    }

    // if (!block_processed[block_num]) {
    //     block_processed[block_num] = 1;
    // }
}

static void process_block(unsigned long block_num, unsigned long total_blocks, unsigned long total_ts_num, buffer_data_inmemory_ekosmas_lf *input_data, isax_index *index, char is_helper, char fai_only_after_help)
{
    unsigned long my_ts_start, my_ts_end, ts_id;

    my_ts_start = block_num * read_block_length;
    if (block_num == total_blocks - 1)
    { // there may still remain some more data series (i.e. less than #read_block_length)
        my_ts_end = total_ts_num;
    }
    else
    {
        my_ts_end = my_ts_start + read_block_length;
    }

    unsigned long total_groups_in_block = (my_ts_end - my_ts_start) / ts_group_length;
    if (total_groups_in_block * ts_group_length < my_ts_end - my_ts_start)
    {
        total_groups_in_block++;
    }

    unsigned long prev_group_id = 0;
    char helpers_exist = 0;

    if (!is_helper)
    {
        COUNT_MY_TIME_START
    }

    while (!block_processed[block_num])
    {
        unsigned long ts_group;

        if (fai_only_after_help && !block_helper_exist[block_num])
        {
            ts_group = next_ts_group_read_in_block[block_num].num;
            // next_ts_group_read_in_block[block_num].num++;                       // EKOSMAS: ERROR: This is problematic, since a ts_group may be lost
            next_ts_group_read_in_block[block_num].num = ts_group + 1; // EKOSMAS: ERROR: This is again problematic, since the counter may return back
        }
        else
        {
            ts_group = __sync_fetch_and_add(&next_ts_group_read_in_block[block_num].num, 1);
        }

        if (ts_group > prev_group_id + 1)
        {                      // performance enhancement
            helpers_exist = 1; // EKOSMAS: helpers_exist can be replaced with block_helper_exist[block_num], after changing the corresponding line during scan_for_unprocessed_blocks in order to set this bit all the time and not only when fai_only_after_help!=0
        }

        unsigned long ts_group_start = my_ts_start + ts_group * ts_group_length;
        unsigned long ts_group_end;

        if (ts_group >= total_groups_in_block)
        {
            break;
        }

        if (ts_group == total_groups_in_block - 1)
        {
            ts_group_end = my_ts_end;
        }
        else
        {
            ts_group_end = ts_group_start + ts_group_length;
        }

        if (ts_group && ts_group <= prev_group_id)
        {
            printf("\nCAUTION: Counter went back!!\n\n");
            fflush(stdout);
        }

        for (unsigned long ts_id = ts_group_start; ts_id < ts_group_end; ts_id++)
        {
            if (block_processed[block_num])
            {
                return;
            }

            if (!ts_processed[ts_id])
            {
                store_isax_in_pRecBuf(input_data, index, ts_id);
                if (!ts_processed[ts_id])
                {
                    ts_processed[ts_id] = 1;
                }
            }
        }

        prev_group_id = ts_group; // performance enhancement
    }

    if ((is_helper || helpers_exist) && !block_processed[block_num])
    { // performance enhancement
        scan_block_for_unprocessed_ts(input_data, index, my_ts_start, my_ts_end, &block_processed[block_num]);
    }
    else
    {
        COUNT_MY_TIME_FOR_BLOCKS_END
        my_num_blocks_processed++;
    }

    if (!block_processed[block_num])
    {
        block_processed[block_num] = 1;
    }
}

static inline void scan_for_unprocessed_blocks(buffer_data_inmemory_ekosmas_lf *input_data, isax_index *index, unsigned long total_blocks, char fai_only_after_help)
{
    unsigned long total_ts_num = input_data->ts_num;
    unsigned long my_id = input_data->workernumber;

    unsigned long start_block_num = (my_id + 1) % total_blocks;
    unsigned long block_num = start_block_num;

    unsigned long backoff_time = backoff_multiplier;

    if (my_num_blocks_processed)
    {
        backoff_time *= (unsigned long)BACKOFF_BLOCK_DELAY_VALUE;
    }
    else
    {
        backoff_time = 0;
    }

    do
    {
        if (all_blocks_processed)
        {
            break;
        }
        else if (block_processed[block_num])
        {
            block_num = (block_num + 1) % total_blocks;
            continue;
        }

        backoff_delay_char(backoff_time, &block_processed[block_num]);

        if (block_processed[block_num])
        {
            blocks_helping_avoided_cnt++;
            block_num = (block_num + 1) % total_blocks;
            continue;
        }

        blocks_helped_cnt++;
        __sync_fetch_and_add(&block_helpers_num[block_num], 1);

        if (fai_only_after_help && !block_helper_exist[block_num])
        {
            block_helper_exist[block_num] = 1;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 1, fai_only_after_help);

        block_num = (block_num + 1) % total_blocks;
    } while (block_num != start_block_num);

    if (!all_blocks_processed)
        all_blocks_processed = 1;

    if (blocks_helping_avoided_cnt)
    {
        COUNT_BLOCK_HELP_AVOIDED(blocks_helping_avoided_cnt)
    }

    if (blocks_helped_cnt)
    {
        COUNT_BLOCKS_HELPED(blocks_helped_cnt)
    }
}

// Lock-Free Full FAI (9992, 99929, 9994, 99949, 9996, 99969, 9998, 99989)
void *index_creation_pRecBuf_worker_new_ekosmas_lock_free_full_fai(void *transferdata)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long total_ts_num = input_data->ts_num;
    unsigned long total_blocks = total_ts_num / read_block_length;
    if (read_block_length * total_blocks < total_ts_num)
    {
        total_blocks++;
    }

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long block_num;
    while (!all_blocks_processed)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if (block_num >= total_blocks)
        {
            break;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 0, 0);
    }

    if (DO_NOT_HELP)
    { // EKOSMAS: ADDED 01/07/2020
        pthread_barrier_wait(input_data->wait_summaries_to_compute);
    }
    else
    {
        scan_for_unprocessed_blocks(input_data, index, total_blocks, 0);
    }

    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
        // printf ("PHASE 2: Index Creation!!\n"); fflush(stdout);
    }

    tree_index_creation_from_pRecBuf_fai_lock_free(transferdata, input_data->parallelism_in_subtree);
}

// Lock-Free FAI (per ts of a block) only after a helper exists (9993, 99939, 9995, 9959, 9997, 99979, 9999, 99999)
void *index_creation_pRecBuf_worker_new_ekosmas_lock_free_fai_only_after_help(void *transferdata)
{
    buffer_data_inmemory_ekosmas_lf *input_data = (buffer_data_inmemory_ekosmas_lf *)transferdata;

    threadPin(input_data->workernumber, maxquerythread);

    unsigned long total_ts_num = input_data->ts_num;
    unsigned long total_blocks = total_ts_num / read_block_length;
    if (read_block_length * total_blocks < total_ts_num)
    {
        total_blocks++;
    }

    isax_index *index = input_data->index;
    unsigned long *next_block_to_process = input_data->shared_start_number;
    unsigned long block_num;
    while (!all_blocks_processed)
    {
        block_num = __sync_fetch_and_add(next_block_to_process, 1);
        if (block_num >= total_blocks)
        {
            break;
        }

        process_block(block_num, total_blocks, total_ts_num, input_data, index, 0, 1);
    }

    if (DO_NOT_HELP)
    { // EKOSMAS: ADDED 01/07/2020
        pthread_barrier_wait(input_data->wait_summaries_to_compute);
    }
    else
    {
        scan_for_unprocessed_blocks(input_data, index, total_blocks, 1);
    }

    if (input_data->workernumber == 0)
    {
        COUNT_FILL_REC_BUF_TIME_END
        COUNT_CREATE_TREE_INDEX_TIME_START
    }

    tree_index_creation_from_pRecBuf_fai_lock_free(transferdata, input_data->parallelism_in_subtree);
}

root_mask_type isax_pRecBuf_index_insert_inmemory(isax_index *index,
                                                  sax_type *sax,
                                                  file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i, t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf(
        (parallel_first_buffer_layer *)(index->fbl),
        sax,
        pos,
        first_bit_mask,
        index,
        lock_firstnode,
        workernumber,
        total_workernumber);

    return first_bit_mask;
}
root_mask_type isax_pRecBuf_index_insert_inmemory_ekosmas(isax_index *index,
                                                          sax_type *sax,
                                                          file_position_type *pos, pthread_mutex_t *lock_firstnode, int workernumber, int total_workernumber)
{
    int i, t;
    int totalsize = index->settings->max_total_buffer_size;

    // Create mask for the first bit of the sax representation

    // Step 1: Check if there is a root node that represents the
    //         current node's sax representation

    // TODO: Create INSERTION SHORT AND BINARY SEARCH METHODS.

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);

    insert_to_pRecBuf_ekosmas(
        (parallel_first_buffer_layer_ekosmas *)(index->fbl),
        sax,
        pos,
        first_bit_mask,
        index,
        lock_firstnode,
        workernumber,
        total_workernumber);

    return first_bit_mask;
}

root_mask_type isax_single_pRecBuf_index_insert_inmemory_geopat(isax_index *index,sax_type *sax,file_position_type *pos, pthread_mutex_t *lock_firstnode,
                                                                int workernumber,int total_workernumber,unsigned long number_of_series, unsigned long time_series_num,int flag)
{
    int i, t;
    int totalsize = index->settings->max_total_buffer_size;

    root_mask_type first_bit_mask = 0x00;

    CREATE_MASK(first_bit_mask, index, sax);


    insert_to_single_pRecBuf_geopat(
        (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl),
        sax,
        pos,
        first_bit_mask,
        index,
        lock_firstnode,
        workernumber,
        total_workernumber,
        number_of_series,
        time_series_num,
        flag);

    return first_bit_mask;
}




enum response flush_subtree_leaf_buffers_inmemory(isax_index *index, isax_node *node)
{

    if (node->is_leaf && node->filename != NULL)
    {
        // Set that unloaded data exist in disk
        if (node->buffer->partial_buffer_size > 0 || node->buffer->tmp_partial_buffer_size > 0)
        {
            node->has_partial_data_file = 1;
        }
        // Set that the node has flushed full data in the disk
        if (node->buffer->full_buffer_size > 0 || node->buffer->tmp_full_buffer_size > 0)
        {
            node->has_full_data_file = 1;
        }

        if (node->has_full_data_file)
        {
            int prev_rec_count = node->leaf_size - (node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size);

            int previous_page_size = ceil((float)(prev_rec_count * index->settings->full_record_size) / (float)PAGE_SIZE);
            int current_page_size = ceil((float)(node->leaf_size * index->settings->full_record_size) / (float)PAGE_SIZE);
            __sync_fetch_and_add(&(index->memory_info.disk_data_full), (current_page_size - previous_page_size));
            // index->memory_info.disk_data_full += (current_page_size - previous_page_size);
        }
        if (node->has_partial_data_file)
        {
            int prev_rec_count = node->leaf_size - (node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size);

            int previous_page_size = ceil((float)(prev_rec_count * index->settings->partial_record_size) / (float)PAGE_SIZE);
            int current_page_size = ceil((float)(node->leaf_size * index->settings->partial_record_size) / (float)PAGE_SIZE);

            // index->memory_info.disk_data_partial += (current_page_size - previous_page_size);
            __sync_fetch_and_add(&(index->memory_info.disk_data_partial), (current_page_size - previous_page_size));
        }
        if (node->has_full_data_file && node->has_partial_data_file)
        {
            printf("WARNING: (Mem size counting) this leaf has both partial and full data.\n");
        }
        // index->memory_info.disk_data_full += (node->buffer->full_buffer_size +
        // node->buffer->tmp_full_buffer_size);
        __sync_fetch_and_add(&(index->memory_info.disk_data_full), (node->buffer->full_buffer_size + node->buffer->tmp_full_buffer_size));
        // index->memory_info.disk_data_partial += (node->buffer->partial_buffer_size +
        // node->buffer->tmp_partial_buffer_size);
        __sync_fetch_and_add(&(index->memory_info.disk_data_partial), (node->buffer->partial_buffer_size + node->buffer->tmp_partial_buffer_size));
        // flush_node_buffer(node->buffer, index->settings->paa_segments,
        // index->settings->timeseries_size,
        // node->filename);
    }
    else if (!node->is_leaf)
    {
        flush_subtree_leaf_buffers_inmemory(index, node->left_child);
        flush_subtree_leaf_buffers_inmemory(index, node->right_child);
    }

    return SUCCESS;
}
