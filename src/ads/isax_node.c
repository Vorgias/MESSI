//
//  isax_node.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//
#include "../../config.h"
#include "../../globals.h"
#include <stdio.h>
#include <stdlib.h>

#include "ads/isax_node.h"

/**
 This function initializes an isax root node.
 */
// EKOSMAS: FUNCTION READ
isax_node * isax_root_node_init(root_mask_type mask, int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_geopat(root_mask_type mask, int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,int max_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_announce(root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce(initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_announce_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce_copy(old_node, initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);
    node->mask = mask;
    return node;
}

isax_node * isax_root_node_init_lockfree_cow(root_mask_type mask, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init_lockfree_cow(initial_buffer_size);
    node->mask = mask;
    return node;
}

isax_node_single_buffer * isax_root_node_init_lockfree_cow_geopat(root_mask_type mask, int initial_buffer_size,int max_buffer_size,int subtree_parallelism) 
{
    isax_node_single_buffer *node = isax_leaf_node_init_lockfree_cow_geopat(initial_buffer_size,max_buffer_size,subtree_parallelism);
    node->node->mask = mask;
    return node;
}


isax_node * isax_root_node_init_lockfree_cow_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init_lockfree_cow_copy(old_node, initial_buffer_size);
    node->mask = mask;
    return node;
}

isax_node_single_buffer * isax_root_node_init_lockfree_cow_copy_geopat(isax_node_single_buffer *old_node, root_mask_type mask, int initial_buffer_size,int max_buffer_size,int subtree_parallelism) 
{
    isax_node_single_buffer *node = isax_leaf_node_init_lockfree_cow_copy_geopat(old_node, initial_buffer_size,max_buffer_size,subtree_parallelism);
    node->node->mask = mask;
    return node;
}


/**
 This function initalizes an isax leaf node.
 */
isax_node * isax_leaf_node_init(int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    // COUNT_NEW_NODE()                                                         // EKOSMAS: REMOVED JUNE 17 2020
    isax_node *node = malloc(sizeof(isax_node));
    if(node == NULL) {
        fprintf(stderr,"error: could not allocate memory for new node.\n");
        return NULL;
    }
    node->has_partial_data_file = 0;
    node->has_full_data_file = 0;
    node->right_child = NULL;
    node->left_child = NULL;
    node->parent = NULL;
    node->next = NULL;
    node->leaf_size = 0;
    node->filename = NULL;
    node->isax_values = NULL;
    node->isax_cardinalities = NULL;
    node->previous = NULL;
    node->split_data = NULL;
    node->buffer = init_node_buffer(initial_buffer_size);
    node->fai_leaf_size = 0;                                                    // EKOSMAS: ADDED 28 JULY 2020
    node->mask = 0;
    node->wedges = NULL;
    node->is_leaf = 0;
    node->lightweight_path = 0;
    node->announce_array = NULL;
    node->recBuf_leaf_helpers_exist = 0;
    node->fbl_node = current_fbl_node;
    node->processed = 0;
    node->lock_node = NULL;                                                 // EKOSMAS: ADDED 02 NOVEMBER 2020
    ///////////////////////////////////////////////////
    //node->lock_node = malloc(sizeof(pthread_mutex_t));
    //pthread_mutex_init(node->lock_node,NULL);
    node->leaf_id = -1;
    node->numofleafs = 0;
    node->rightmost_leaf = NULL;
    node->leftmost_leaf = NULL;
    node->leaflist_previous = NULL;
    node->leaflist_next = NULL;
    node->nextInSkiplist = NULL;
    node->supports_pred = 0;
    node->attribute_when_searchedFirst = NULL;
    node->kdtree=NULL;
    ///////////////////////////////////////////////////
    return node;
}

isax_node_single_buffer * isax_leaf_node_init_geopat(int initial_buffer_size,int max_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,int subtree_parallelism) 
{
    // COUNT_NEW_NODE()                                                         // EKOSMAS: REMOVED JUNE 17 2020
    isax_node_single_buffer *n = malloc(sizeof(isax_node_single_buffer));
    if(n == NULL) {
        fprintf(stderr,"error: could not allocate memory for new node.\n");
        return NULL;
    }
    n->node = isax_leaf_node_init(initial_buffer_size,current_fbl_node);

    n->isVisited = 0;
    n->isHelper = 0;
    n->leftSubtreeDone = 0;
    n->rightSubtreeDone = 0;
    n->leftSubtreeCounter = 0;
    n->rightSubtreeCounter = 0;
    n->subTreeNodesNum = 0;
    //
    n->node_counter = 1;

    if(subtree_parallelism == 1){
        n->node->buffer->partial_sax_buffer = malloc(sizeof(sax_type*)*2000);
        n->node->buffer->partial_position_buffer = malloc(sizeof(file_position_type*)*2000);
        for(int i = 0 ;i<2000;i++){
            n->node->buffer->partial_sax_buffer[i] = NULL;
            n->node->buffer->partial_position_buffer[i] = NULL;
        }
    }
    
    n->update = malloc(sizeof(Update));
    n->update->info = NULL;
    n->update->state = CLEAN;
                                                                     
    return n;
}




isax_node * isax_leaf_node_init_lockfree_announce(int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init_lockfree_announce_copy(NULL, initial_buffer_size, total_workers_num, lockfree_parallelism_in_subtree, current_fbl_node);

    node->buffer->partial_position_buffer = calloc(node->buffer->initial_buffer_size, sizeof(file_position_type*));
    node->buffer->partial_sax_buffer = calloc(node->buffer->initial_buffer_size, sizeof(sax_type*));
    node->is_leaf = 1;

    return node;
}

isax_node * isax_leaf_node_init_lockfree_announce_copy(isax_node *old_node, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, current_fbl_node);

    if (old_node && old_node->isax_values) {
        node->isax_values = old_node->isax_values;
        node->isax_cardinalities= old_node->isax_cardinalities;
    }

    if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
        node->announce_array = calloc(total_workers_num, sizeof(announce_rec *));
    }

    return node;
}

announce_rec *create_new_announce_rec(isax_node_record *record) {
    announce_rec *new_rec = malloc (sizeof(announce_rec));
    new_rec->record.sax = record->sax;
    new_rec->record.position = record->position;
    new_rec->buf_pos = ULONG_MAX;
    return (new_rec);
}

isax_node * isax_leaf_node_init_lockfree_cow(int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);
    node->is_leaf = 1;

    return node;
}

isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_geopat(int initial_buffer_size,int max_buffer_size,int subtree_parallelism) 
{
    isax_node_single_buffer *node = isax_leaf_node_init_geopat(initial_buffer_size,max_buffer_size, NULL,subtree_parallelism);
    node->node->is_leaf = 1;

    return node;
}


isax_node * isax_leaf_node_init_lockfree_cow_copy(isax_node *old_node, int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);

    if (old_node && old_node->isax_values) {
        node->isax_values = old_node->isax_values;
        node->isax_cardinalities= old_node->isax_cardinalities;
    }

    return node;
}

isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_copy_geopat(isax_node_single_buffer *old_node, int initial_buffer_size,int max_buffer_size,int subtree_parallelism) 
{
    isax_node_single_buffer *node = isax_leaf_node_init_geopat(initial_buffer_size,max_buffer_size, NULL,subtree_parallelism);

    if (old_node->node && old_node->node->isax_values ) {
        node->node->isax_values = old_node->node->isax_values;
        node->node->isax_cardinalities= old_node->node->isax_cardinalities;      
    }

    return node;
}

isax_node * isax_leaf_node_init_lockfree_cow_split(int initial_buffer_size) 
{
    isax_node *node = isax_leaf_node_init(initial_buffer_size, NULL);
    node->is_leaf = 1;

    node->buffer->partial_position_buffer = malloc(initial_buffer_size * sizeof(file_position_type*));
    node->buffer->partial_sax_buffer = malloc(initial_buffer_size * sizeof(sax_type*));

    return node;
}

isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_split_geopat(int initial_buffer_size,int max_buffer_size,int subtree_parallelism) 
{
    isax_node_single_buffer *node = isax_leaf_node_init_geopat(initial_buffer_size,max_buffer_size, NULL,subtree_parallelism);
    node->node->is_leaf = 1;

    if(subtree_parallelism !=1 ){
        node->node->buffer->partial_position_buffer = malloc(2000 * sizeof(file_position_type*));
        node->node->buffer->partial_sax_buffer = malloc(2000 * sizeof(sax_type*));
    }
    

    return node;
}