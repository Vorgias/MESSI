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
#include "ads/isax_query_engine.h"
#include "ads/inmemory_query_engine.h"
#include "ads/parallel_query_engine.h"
#include "ads/parallel_inmemory_query_engine.h"
#include "ads/parallel_index_engine.h"
#include "ads/isax_first_buffer_layer.h"
#include "ads/pqueue.h"
#include "ads/sax/sax.h"
#include "ads/isax_node_split.h"

query_result  approximate_search_inmemory_pRecBuf(ts_type *ts, ts_type *paa, isax_index *index) 
{
    query_result result;

    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments,
                 index->settings->sax_alphabet_cardinality,
                 index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    if ((&((parallel_first_buffer_layer*)(index->fbl))->soft_buffers[(int) root_mask])->initialized) {
        isax_node *node = (isax_node *)(&((parallel_first_buffer_layer*)(index->fbl))->soft_buffers[(int) root_mask])->node;
        // Traverse tree

        // Adaptive splitting

        while (!node->is_leaf) {
            int location = index->settings->sax_bit_cardinality - 1 -
            node->split_data->split_mask[node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];

            if(sax[node->split_data->splitpoint] & mask)
            {
                node = node->right_child;
            }
            else
            {
                node = node->left_child;
            }

            // Adaptive splitting
        }
        result.distance = calculate_node_distance_inmemory(index, node, ts, FLT_MAX);           // caclulate initial BSF
        result.node = node;
    }
    else {
        printf("approximate_search_inmemory_pRecBuf: NO BSF has been computed! Bad Luck...\n");fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}

//////////////////////////////////////
int leafContainsAttr(isax_node* root , int key){
    for(int i = 0; i<root->buffer->partial_buffer_size;i++){
        if(*(root->buffer->partial_attribute_buffer[i])==key)return 1;
    }
    return 0;
}

int evalSubtree(isax_node* root , int key){
    
    if(root->is_leaf)return leafContainsAttr(root,key);
    isax_node * temp = root->leftmost_leaf;
    int max = root->rightmost_leaf->leaf_id;
    printf("starting from : %d to %d\n",temp->leaf_id,max);
    while(temp!=NULL && temp->leaf_id <= max){
        if(leafContainsAttr(temp,key))return 1;
        temp = temp->leaflist_next;
    }
    return 0;

}
//////////////////////////////////////
query_result  approximate_search_inmemory_pRecBuf_ekosmas(ts_type *ts, ts_type *paa, isax_index *index) 
{
    /////////////////////////////
    int key = 1;
    /////////////////////////////
    query_result result;
    
    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments,
                 index->settings->sax_alphabet_cardinality,
                 index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    if ((&((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[(int) root_mask])->initialized) {
        isax_node *node = (isax_node *)(&((parallel_first_buffer_layer_ekosmas*)(index->fbl))->soft_buffers[(int) root_mask])->node;
        
        // Traverse tree
        while (!node->is_leaf) {
            int location = index->settings->sax_bit_cardinality - 1 -
            node->split_data->split_mask[node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];

            if(sax[node->split_data->splitpoint] & mask)
            {
                //////////////////////////////////
                if(evalSubtree(node->right_child,key)){
                //////////////////////////////////
                node = node->right_child;
                ///////////////
                }else{
                    //printf("wanted right went left\n");
                    node = node->left_child;
                }
                ///////////////
            }
            else
            {
                //////////////////////////////////
                if(evalSubtree(node->left_child,key)){
                //////////////////////////////////
                node = node->left_child;
                ///////////////////
                }else{
                    //printf("wanted left went right");
                    node = node->right_child;
                }
                ///////////////////
            }
        }
        result.distance = calculate_node_distance_inmemory_with_attribute(index, node, ts, FLT_MAX,key);           // caclulate initial BSF
        result.node = node;
    }
    else {
        printf("approximate_search_inmemory_pRecBuf_ekosmas: NO BSF has been computed! Bad Luck...\n");fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}

query_result  approximate_search_inmemory_pRecBuf_ekosmas_lf(ts_type *ts, ts_type *paa, isax_index *index, const char parallelism_in_subtree) 
{
    query_result result;

    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments,
                 index->settings->sax_alphabet_cardinality,
                 index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node = &((parallel_first_buffer_layer_ekosmas_lf*)(index->fbl))->soft_buffers[(int) root_mask];
    if (current_fbl_node && current_fbl_node->initialized) {
        isax_node *node = current_fbl_node->node;

        // Traverse tree
        while (!node->is_leaf) {
            int location = index->settings->sax_bit_cardinality - 1 -
            node->split_data->split_mask[node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];

            if(sax[node->split_data->splitpoint] & mask)
            {
                node = node->right_child;
            }
            else
            {
                node = node->left_child;
            }
        }
        result.distance = calculate_node_distance_inmemory_ekosmas_lf(index, node, ts, FLT_MAX, parallelism_in_subtree);           // caclulate initial BSF
        result.node = node;
    }
    else {
        printf("approximate_search_inmemory_pRecBuf_ekosmas_lf: NO BSF has been computed! Bad Luck...\n");fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);
    return result;
}

//geopat
query_result  approximate_search_inmemory_pRecBuf_ekosmas_lf_geopat(ts_type *ts, ts_type *paa, isax_index *index, const char parallelism_in_subtree) 
{
    query_result result;

    sax_type *sax = malloc(sizeof(sax_type) * index->settings->paa_segments);
    sax_from_paa(paa, sax, index->settings->paa_segments,
                 index->settings->sax_alphabet_cardinality,
                 index->settings->sax_bit_cardinality);

    root_mask_type root_mask = 0;
    CREATE_MASK(root_mask, index, sax);

    parallel_first_buffer_layer_ekosmas_lf_geopat *fbl = (parallel_first_buffer_layer_ekosmas_lf_geopat *)(index->fbl);
    parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node = (fbl->soft_buffers);

    if (current_fbl_node->node[root_mask] != NULL) {
        isax_node_single_buffer *node = current_fbl_node->node[(unsigned long )root_mask];

        // Traverse tree
        while (!node->node->is_leaf) {
            int location = index->settings->sax_bit_cardinality - 1 -
            node->node->split_data->split_mask[node->node->split_data->splitpoint];
            root_mask_type mask = index->settings->bit_masks[location];
            if(sax[node->node->split_data->splitpoint] & mask)
            {
                node = node->node->right_child;
            }
            else
            {
                node = node->node->left_child;
            }
        }
        result.distance = calculate_node_distance_inmemory_geopat_lf(index, node, ts, FLT_MAX, parallelism_in_subtree);           // caclulate initial BSF
        result.node = node;
    }
    else {
        printf("approximate_search_inmemory_pRecBuf_ekosmas_lf_geopat: NO BSF has been computed! Bad Luck...\n");fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}






float calculate_node_distance_inmemory (isax_index *index, isax_node *node, ts_type *query, float bsf) 
{
    COUNT_CHECKED_NODE()
    // If node has buffered data

    if (node->buffer != NULL) 
    {   
        int i;
        for (i=0; i<node->buffer->full_buffer_size; i++) 
        {
            float dist = ts_euclidean_distance(query, node->buffer->full_ts_buffer[i], 
                                               index->settings->timeseries_size, bsf);
            if (dist < bsf) {
                bsf = dist;
            }
        }

        for (i=0; i<node->buffer->tmp_full_buffer_size; i++) {
            float dist = ts_euclidean_distance(query, node->buffer->tmp_full_ts_buffer[i], 
                                               index->settings->timeseries_size, bsf);
            if (dist < bsf ) {
                bsf = dist;
            }
        }
       // RDcalculationnumber=RDcalculationnumber+node->buffer->partial_buffer_size;
        for (i=0; i<node->buffer->partial_buffer_size; i++) {

            float dist = ts_euclidean_distance_SIMD(query, &(rawfile[*node->buffer->partial_position_buffer[i]]), 
                                               index->settings->timeseries_size, bsf);

            if (dist < bsf) {
                bsf = dist;

            }
        }
    }
    
    return bsf;
}
////////////////////////////////////
float calculate_node_distance_inmemory_with_attribute (isax_index *index, isax_node *node, ts_type *query, float bsf,int key) 
{
    // If node has buffered data
    if (node->buffer != NULL) 
    {   
        for (int i=0; i<node->buffer->partial_buffer_size; i++) {
            if(*node->buffer->partial_attribute_buffer[i] == key){
            float dist = ts_euclidean_distance_SIMD(query, &(rawfile[*node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf);

            if (dist < bsf) {
                bsf = dist;

            }
            }
        }
    }
    
    return bsf;
}
/////////////////////////////////////
float calculate_node_distance_inmemory_ekosmas (isax_index *index, isax_node *node, ts_type *query, float bsf) 
{
    // If node has buffered data
    if (node->buffer != NULL) 
    {   
        for (int i=0; i<node->buffer->partial_buffer_size; i++) {

            float dist = ts_euclidean_distance_SIMD(query, &(rawfile[*node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf);

            if (dist < bsf) {
                bsf = dist;

            }
        }
    }
    
    return bsf;
}
float calculate_node_distance_inmemory_ekosmas_lf (isax_index *index, isax_node *node, ts_type *query, float bsf, const char parallelism_in_subtree) 
{
    // If node has buffered data
    if (node->buffer != NULL) 
    {   
        int size;

        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && ((parallel_fbl_soft_buffer_ekosmas_lf *)(node->fbl_node))->recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist)) {
            if (node->fai_leaf_size == 0) {
                size = node->leaf_size;
            }
            size = node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW) {
            size = node->buffer->partial_buffer_size;
        }
        else {
            size = node->leaf_size;
        }
        //printf("Size = %d \n",size);
        ts_type * tmp = NULL;
        int count = 0 ;
        for (int i=0; i < size; i++) {

            float dist = ts_euclidean_distance(query, &(rawfile[*node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf);


            if (dist < bsf ) {
                bsf = dist;

            }
        }

    }
    printf("BSF result = %f\n",bsf);
    return bsf;
}


float calculate_node_distance_inmemory_geopat_lf (isax_index *index, isax_node_single_buffer *node, ts_type *query, float bsf, const char parallelism_in_subtree) 
{
    // If node has buffered data
    if (node->node->buffer!= NULL) 
    {   
        int size = 0;

        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && ((parallel_fbl_soft_buffer_ekosmas_lf *)(node->node->fbl_node))->recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->node->recBuf_leaf_helpers_exist)) {
            if (node->node->fai_leaf_size == 0) {
                size = node->node->leaf_size;
            }
            size = node->node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_FLAG){
            size = node->node->buffer->partial_buffer_size;   
        }
        else {
            size = node->node->leaf_size;
        }
        for (int i=0; i < size; i++) {
            float dist = ts_euclidean_distance(query, &(rawfile[*node->node->buffer->partial_position_buffer[i]]), index->settings->timeseries_size, bsf);
            if (dist < bsf) {
                bsf = dist;

            }
        }
    }
   
    return bsf;
}


float calculate_node_distance2_inmemory (isax_index *index, isax_node *node, ts_type *query,ts_type *paa, float bsf) 
{
    COUNT_CHECKED_NODE()
    float distmin;
    // If node has buffered data
    if (node->buffer != NULL) 
    {
        int i;
        
        for (i=0; i<node->buffer->partial_buffer_size; i++) {

            distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                               index->settings->max_sax_cardinalities,
                                               index->settings->sax_bit_cardinality,
                                               index->settings->sax_alphabet_cardinality,
                                               index->settings->paa_segments, MINVAL, MAXVAL,
                                               index->settings->mindist_sqrt);
            if (distmin<bsf)
            {
                float dist = ts_euclidean_distance_SIMD(query, 
                                                        &(rawfile[*node->buffer->partial_position_buffer[i]]), 
                                                        index->settings->timeseries_size, 
                                                        bsf);
                if (dist < bsf) {
                    bsf = dist;

                }  
            }
        }
    }
    
    
    return bsf;
}
float calculate_node_distance2_inmemory_ekosmas (isax_index *index, isax_node *node, ts_type *query, ts_type *paa, float bsf) 
{
    // If node has buffered data
    if (node->buffer != NULL) 
    {
        for (int i=0; i<node->buffer->partial_buffer_size; i++) {

            float distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                               index->settings->max_sax_cardinalities,
                                               index->settings->sax_bit_cardinality,
                                               index->settings->sax_alphabet_cardinality,
                                               index->settings->paa_segments, MINVAL, MAXVAL,
                                               index->settings->mindist_sqrt);
            if (distmin<bsf) {
                float dist = ts_euclidean_distance_SIMD(query, 
                                                        &(rawfile[*node->buffer->partial_position_buffer[i]]), 
                                                        index->settings->timeseries_size, 
                                                        bsf);
                if (dist < bsf) {
                    bsf = dist;
                }  
            }
        }
    }
    
    
    
    return bsf;
}

 float calculate_eyclidian_distance(isax_index *index, ts_type *query, float bsf,unsigned long position){
                                                                                   
                float dist = ts_euclidean_distance(query, 
                                                        &(rawfile[position * index->settings->timeseries_size]), 
                                                        index->settings->timeseries_size, 
                                                        bsf);
                return dist;
}


float calculate_node_distance2_inmemory_geopat2(isax_index *index, query_result *n, ts_type *query, ts_type *paa, float bsf,const char parallelism_in_subtree) 
{


    isax_node_single_buffer *node = (isax_node_single_buffer *)n->node;

    // If node has buffered data
    if (node->node->is_leaf) 
    {   
        int size = 0 ;

        size =  node->node->buffer->partial_buffer_size;
        
        for (int i=0; i < size && n->distance >=0; i++) {

            float distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->node->buffer->partial_sax_buffer[i],
                                               index->settings->max_sax_cardinalities,
                                               index->settings->sax_bit_cardinality,
                                               index->settings->sax_alphabet_cardinality,
                                               index->settings->paa_segments, MINVAL, MAXVAL,
                                               index->settings->mindist_sqrt);
            
                                                                                             
            if (distmin<bsf) {
                
                float dist = ts_euclidean_distance_SIMD(query, 
                                                        &(rawfile[*node->node->buffer->partial_position_buffer[i]]), 
                                                        index->settings->timeseries_size, 
                                                        bsf);
                if (dist < bsf) {
                    //printf("Position = %d,  Distance = %f \n", *node->buffer_rc.s.buffer->partial_position_buffer[i],distmin);
                    bsf = dist;
                }
            }
        }
    }
  
    return bsf;
}



float calculate_node_distance2_inmemory_ekosmas_lf (isax_index *index, query_result *n, ts_type *query, ts_type *paa, float bsf, const char parallelism_in_subtree) 
{

    // printf ("calculate_node_distance2_inmemory_ekosmas_lf - START\n"); fflush(stdout);

    isax_node *node = n->node;

    // If node has buffered data
    if (node->buffer != NULL) 
    {   
        int size;

        // printf ("calculate_node_distance2_inmemory_ekosmas_lf - node->buffer != NULL\n"); fflush(stdout);

        if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && ((parallel_fbl_soft_buffer_ekosmas_lf *)(node->fbl_node))->recBuf_helpers_exist) ||
            (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist)) {
            if (node->fai_leaf_size == 0) {
                size = node->leaf_size;

            }
            size = node->fai_leaf_size;
        }
        else if (parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_COW) {
            size = node->buffer->partial_buffer_size;
        }
        else {
            size = node->leaf_size;
        }

        // printf ("calculate_node_distance2_inmemory_ekosmas_lf - nosize = [%d]\n", size); fflush(stdout);

        for (int i=0; i < size && n->distance >=0; i++) {

            // printf ("calculate_node_distance2_inmemory_ekosmas_lf - iteratiom [%d] - START\n", i); fflush(stdout);
            
            if (!node->buffer->partial_sax_buffer[i]) {
                // printf ("calculate_node_distance2_inmemory_ekosmas_lf - iteratiom [%d] - SKIP\n", i); fflush(stdout);
                continue;
            }

            float distmin = minidist_paa_to_isax_rawa_SIMD(paa, node->buffer->partial_sax_buffer[i],
                                               index->settings->max_sax_cardinalities,
                                               index->settings->sax_bit_cardinality,
                                               index->settings->sax_alphabet_cardinality,
                                               index->settings->paa_segments, MINVAL, MAXVAL,
                                               index->settings->mindist_sqrt);
            // printf ("calculate_node_distance2_inmemory_ekosmas_lf - distmin = [%f]\n", distmin); fflush(stdout);

            if (distmin<bsf && n->distance >=0) {
                float dist = ts_euclidean_distance_SIMD(query, 
                                                        &(rawfile[*node->buffer->partial_position_buffer[i]]), 
                                                        index->settings->timeseries_size, 
                                                        bsf);

                // printf ("calculate_node_distance2_inmemory_ekosmas_lf - ts_euclidean_distance_SIMD - dist = [%f]\n", dist); fflush(stdout);

                if (dist < bsf) {
                    // printf ("calculate_node_distance2_inmemory_ekosmas_lf - dist[%f] is smaller than bsf[%f]\n", dist, bsf); fflush(stdout);
                    bsf = dist;

                }
            }

            // printf ("calculate_node_distance2_inmemory_ekosmas_lf - iteratiom [%d] - END\n", i); fflush(stdout);

        }
    }

    // printf ("calculate_node_distance2_inmemory_ekosmas_lf - END\n"); fflush(stdout);
    
    return bsf;
}
