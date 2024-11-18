//
//  isax_node.h
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//

#ifndef isaxlib_isax_node_h
#define isaxlib_isax_node_h

struct isax_node;

#include "../../config.h"
#include "../../globals.h"
#include "isax_node_record.h"
#include "isax_node_buffer.h"

typedef struct isax_node_split_data {
    int splitpoint;
    sax_type * split_mask;
} isax_node_split_data;

typedef struct {
    isax_node_record record;
    unsigned long buf_pos;
} announce_rec;

typedef struct InfoRecord InfoRecord ;

typedef enum {IFLAG,CLEAN} STATE;


typedef struct update {
    STATE state ;
    InfoRecord *info; 
} Update;

///////////////////////////////
typedef struct skipSearch_node{
    attribute_type * attribute;
    int searched;
    int supports_predicate;
    struct isax_node * lastSearchedInARow;
    struct skipSearch_node* next;//in same node different query
}skipSearch_node;
///////////////////////////////


typedef struct isax_node {
    // General
    int leaf_size;
    char has_partial_data_file;
    char has_full_data_file;
    
    sax_type * isax_values;
    sax_type * isax_cardinalities;
    
    struct isax_node *next;
    struct isax_node *previous;
    root_mask_type mask;
    void *parent;
    
    // If is leaf
    unsigned char is_leaf; 
    char * filename;
    isax_node_buffer *buffer;
    // FAI object
    volatile unsigned long fai_leaf_size;       // EKOSMAS: ADDED 28 JULY 2020
    
    // If is intermediate
    struct isax_node_split_data *split_data;
    void *left_child;
    void *right_child;
    
    // Wedges
    ts_type *wedges;

    // mutexes
    pthread_mutex_t *lock_node;                 // EKOSMAS: ADDED 07 JULY 2020

    // Announce array
    volatile announce_rec * volatile *announce_array;       // EKOSMAS: ADDED JULY 30, 2020

    // LightWeight Path Flag
    volatile unsigned char lightweight_path;

    volatile unsigned char recBuf_leaf_helpers_exist;       // EKOSMAS: ADDED AUGUST 24, 2020

    void *fbl_node;                                         // EKOSMAS: ADDED SEPTEMBER 01, 2020

    volatile unsigned char processed;                       // EKOSMAS: ADDED SEPTEMBER 03, 2020

    /////////////////////////////////////
    struct isax_node * leftmost_leaf;
    struct isax_node * rightmost_leaf;
    //if root
    int numofleafs;
    //if leaf
    struct isax_node *leaflist_next;
    struct isax_node *leaflist_previous;
    int leaf_id;
    //skiplist 
    struct isax_node * nextInSkiplist;
    int supports_pred;
    attribute_type* attribute_when_searchedFirst;
    ////////////////////////////////////

} isax_node;

typedef struct isax_node_single_buffer {
    // General
    isax_node *node;

    //vars for subtrees nodes counting
    long isVisited;
    long leftSubtreeDone;
    long rightSubtreeDone;
    long leftSubtreeCounter;
    long rightSubtreeCounter;
    long subTreeNodesNum;
    long isHelper;
    //var for fac 
    unsigned long node_counter;
    int *rootProcessedNodes;
    int rootProcessedSubtree;
    Update *update;

} isax_node_single_buffer;



typedef struct InfoRecord {
    isax_node_single_buffer *leaf;
    isax_node_single_buffer *parent;
    isax_node_record *record;
    long position;
    unsigned long fbl_position;
} InfoRecord;

typedef struct parallel_fbl_soft_buffer_ekosmas_lf parallel_fbl_soft_buffer_ekosmas_lf;
typedef struct parallel_fbl_soft_buffer_ekosmas_lf_geopat parallel_fbl_soft_buffer_ekosmas_lf_geopat;

isax_node * isax_root_node_init(root_mask_type mask, int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node * isax_root_node_init_lockfree_announce(root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node * isax_root_node_init_lockfree_announce_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node * isax_root_node_init_lockfree_cow(root_mask_type mask, int initial_buffer_size);
isax_node_single_buffer * isax_root_node_init_lockfree_cow_geopat(root_mask_type mask, int initial_buffer_size,int max_buffer_size,int subtree_parallelism) ;
isax_node * isax_root_node_init_lockfree_cow_copy(isax_node *old_node, root_mask_type mask, int initial_buffer_size);
isax_node_single_buffer * isax_root_node_init_lockfree_cow_copy_geopat(isax_node_single_buffer *old_node, root_mask_type mask, int initial_buffer_size,int max_buffer_size,int subtree_parallelism);

isax_node * isax_leaf_node_init(int initial_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node_single_buffer * isax_leaf_node_init_geopat(int initial_buffer_size,int max_buffer_size, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,int subtree_parallelism);
isax_node * isax_leaf_node_init_lockfree_announce(int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node * isax_leaf_node_init_lockfree_announce_copy(isax_node *old_node, int initial_buffer_size, unsigned long total_workers_num, const char lockfree_parallelism_in_subtree, parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node);
isax_node * isax_leaf_node_init_lockfree_cow(int initial_buffer_size);
isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_geopat(int initial_buffer_size,int max_buffer_size,int subtree_parallelism);
isax_node * isax_leaf_node_init_lockfree_cow_copy(isax_node *old_node, int initial_buffer_size);
isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_copy_geopat(isax_node_single_buffer *old_node, int initial_buffer_size,int max_buffer_size,int subtree_parallelism);
isax_node * isax_leaf_node_init_lockfree_cow_split(int initial_buffer_size);
isax_node_single_buffer * isax_leaf_node_init_lockfree_cow_split_geopat(int initial_buffer_size,int max_buffer_size,int subtree_parallelism);


announce_rec *create_new_announce_rec(isax_node_record *record);



#endif
