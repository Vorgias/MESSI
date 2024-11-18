//
//  isax_index.c
//  isaxlib
//
//  Created by Kostas Zoumpatianos on 3/10/12.
//  Copyright 2012 University of Trento. All rights reserved.
//


/*
 ============= NOTES: =============
 Building a mask for the following sax word:
 SAX:
 00
 00
 01
 00
 11
 01
 10
 11

 How to build a mask for the FIRST bit of this word (root),
 I use do:
 R = 00000000
 IF(00 AND 10) R = R OR 10000000
 IF(00 AND 10) R = R OR 01000000
 IF(01 AND 10) R = R OR 00100000
 IF(00 AND 10) R = R OR 00010000
 IF(11 AND 10) R = R OR 00001000
 IF(01 AND 10) R = R OR 00000100
 IF(10 AND 10) R = R OR 00000010
 IF(11 AND 10) R = R OR 00000001
 result: R = 00001011


 *** IN ORDER TO CALCULATE LOCATION BITMAP MASKS ***:

 m = 2^NUMBER_OF_MASKS     (e.g for 2^3=8 = 100)
 m>> for the second mask   (e.g.            010)
 m>>>> for the third ...   (e.g.            001)
*/
#ifdef VALUES
#include <values.h>
#endif

#include "../../config.h"
#include "../../globals.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <float.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdbool.h>

#include "ads/sax/sax.h"
#include "ads/sax/ts.h"
#include "ads/isax_index.h"
#include "ads/isax_node.h"
#include "ads/isax_node_buffer.h"
#include "ads/isax_node_split.h"
#include "ads/isax_first_buffer_layer.h"
 #include "ads/inmemory_query_engine.h"
#include "ads/pqueue.h"


int comp(const void * a, const void * b) 
{
    isax_node_record *ra = (isax_node_record*) a;
    isax_node_record *rb = (isax_node_record*) b;
    
    if (*ra->position==*rb->position)
        return 0;
    else
        if (*ra->position < *rb->position)
            return -1;
        else
            return 1;
}


/**
 This function initializes the settings of an isax index
 */
// EKOSMAS: FUNCTION READ
isax_index_settings * isax_index_settings_init(const char * root_directory, int timeseries_size,
                                               int paa_segments, int sax_bit_cardinality,
                                               int max_leaf_size, int min_leaf_size,
                                               int initial_leaf_buffer_size,
                                               int max_total_buffer_size, int initial_fbl_buffer_size,
                                               int total_loaded_leaves, int tight_bound, int aggressive_check, int new_index,char inmemory_flag,int attribute_size,int attribute_max_value,int attribute_min_value)
{
    int i;
    isax_index_settings *settings = malloc(sizeof(isax_index_settings));
    if(settings == NULL) {
        fprintf(stderr,"error: could not allocate memory for index settings.\n");
        return NULL;
    }

    if(new_index) {
		if(chdir(root_directory) == 0)
		{
			fprintf(stderr, "WARNING! Target index directory already exists. Please delete or choose a new one.\n");
		}
        if (!inmemory_flag)
        {
            mkdir(root_directory, 0777);
        }

        settings->max_total_full_buffer_size = max_total_buffer_size;
        settings->initial_fbl_buffer_size = initial_fbl_buffer_size;
    }
    else {
    	if(chdir(root_directory) != 0)
		{
			fprintf(stderr, "WARNING! Target index directory does not exist!\n");
		}
    	else {
    		chdir("../");
    	}
        settings->max_total_full_buffer_size = max_total_buffer_size;
        settings->initial_fbl_buffer_size = initial_fbl_buffer_size;
        //settings->max_total_full_buffer_size = 0;
        //settings->initial_fbl_buffer_size = 0;
    }

    if(paa_segments > (int)(8 * (int)sizeof(root_mask_type))){
        fprintf(stderr,"error: Too many paa segments. The maximum value is %zu.\n",
                8 * sizeof(root_mask_type));
        return NULL;
    }

    if(initial_leaf_buffer_size < max_leaf_size)
    {
        fprintf(stderr,"error: Leaf buffers should be at least as big as leafs.\n");
        return NULL;
    }
    settings->attribute_size = attribute_size;/////////////////////////////////
    settings->attribute_max_value = attribute_max_value;////////////////////////////////
    settings->attribute_min_value = attribute_min_value;///////////////////////////////
    settings->total_loaded_leaves = total_loaded_leaves;
    settings->root_directory = root_directory;
    settings->raw_filename = NULL;

    settings->timeseries_size = timeseries_size;
    settings->paa_segments = paa_segments;
    settings->ts_values_per_paa_segment = timeseries_size/paa_segments;
    settings->max_leaf_size = max_leaf_size;
    settings->min_leaf_size = min_leaf_size;
    settings->initial_leaf_buffer_size = initial_leaf_buffer_size;


	settings->tight_bound = tight_bound;
    settings->aggressive_check = aggressive_check;

    settings->sax_byte_size = (sizeof(sax_type) * paa_segments);
    settings->ts_byte_size = (sizeof(ts_type) * timeseries_size);
    settings->position_byte_size = sizeof(file_position_type);

    settings->full_record_size = settings->sax_byte_size
                                 + settings->position_byte_size
                                 + settings->ts_byte_size;
    settings->partial_record_size = settings->sax_byte_size
                                    + settings->position_byte_size;

    settings->sax_bit_cardinality = sax_bit_cardinality;
    settings->sax_alphabet_cardinality = pow(2, sax_bit_cardinality);

	settings->max_sax_cardinalities = malloc(sizeof(sax_type) * settings->paa_segments);
	for(i=0; i<settings->paa_segments;i++)
		settings->max_sax_cardinalities[i] = settings->sax_bit_cardinality;

    //settings->mindist_sqrt = sqrtf((float) settings->timeseries_size /
    //                               (float) settings->paa_segments);
    settings->mindist_sqrt = ((float) settings->timeseries_size /
                                   (float) settings->paa_segments);
    settings->root_nodes_size = pow(2, settings->paa_segments);

    // SEGMENTS * (CARDINALITY)
    float c_size = ceil(log10(settings->sax_alphabet_cardinality + 1));
    settings->max_filename_size = settings->paa_segments *
                                  ((c_size * 2) + 2)
                                  + 5 + strlen(root_directory);


    if(paa_segments > sax_bit_cardinality)
    {
        settings->bit_masks = malloc(sizeof(root_mask_type) * (paa_segments+1));
        if(settings->bit_masks == NULL) {
            fprintf(stderr,"error: could not allocate memory for bit masks.\n");
            return NULL;
        }

        for (; paa_segments>=0; paa_segments--)
        {
            settings->bit_masks[paa_segments] = pow(2, paa_segments);
        }
    }
    else
    {
        settings->bit_masks = malloc(sizeof(root_mask_type) * (sax_bit_cardinality+1));
        if(settings->bit_masks == NULL) {
            fprintf(stderr,"error: could not allocate memory for bit masks.\n");
            return NULL;
        }

        for (; sax_bit_cardinality>=0; sax_bit_cardinality--)
        {
            settings->bit_masks[sax_bit_cardinality] = pow(2, sax_bit_cardinality);
        }
    }

    if(new_index) {
        // EKOSMAS: There is no use of the following!
        settings->max_total_buffer_size = (int) ((float)(settings->full_record_size/
                                       (float)settings->partial_record_size) * settings->max_total_full_buffer_size);
    }
    else {
        settings->max_total_buffer_size = settings->max_total_full_buffer_size;
    }

    
    return settings;
}


// Botao's version
isax_index * isax_index_init_inmemory(isax_index_settings *settings)
{
    isax_index *index = malloc(sizeof(isax_index));
    if(index == NULL) {
        fprintf(stderr,"error: could not allocate memory for index structure.\n");
        return NULL;
    }
    index->memory_info.mem_tree_structure = 0;
    index->memory_info.mem_data = 0;
    index->memory_info.mem_summaries = 0;
    index->memory_info.disk_data_full = 0;
    index->memory_info.disk_data_partial = 0;

    index->settings = settings;
    index->first_node = NULL;

    // EKOSMAS: Is the following required? - REMOVED it! 10 JULY, 2020
    // if yes, for paraller receive buffers, shouldn't it be:
    // index->fbl = initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
    //                             pow(2, index->settings->paa_segments),
    //                             index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    index->fbl = initialize_fbl(settings->initial_fbl_buffer_size,
                                pow(2, settings->paa_segments),
                                settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);

    index->sax_cache = NULL;

    index->total_records = 0;
    index->loaded_records = 0;

    index->root_nodes = 0;
    index->allocated_memory = 0;
    index->has_wedges = 0;
    //index->locations = malloc(sizeof(int) * settings->timeseries_size);

    index->answer = malloc(sizeof(ts_type) * settings->timeseries_size);
    return index;
}

// EKOSMAS version
isax_index * isax_index_init_inmemory_ekosmas(isax_index_settings *settings)
{
    isax_index *index = malloc(sizeof(isax_index));
    if(index == NULL) {
        fprintf(stderr,"error: could not allocate memory for index structure.\n");
        return NULL;
    }
    // EKOSMAS: REMOVED
    // index->memory_info.mem_tree_structure = 0;
    // index->memory_info.mem_data = 0;
    // index->memory_info.mem_summaries = 0;
    // index->memory_info.disk_data_full = 0;
    // index->memory_info.disk_data_partial = 0;

    index->settings = settings;
    index->first_node = NULL;

    // EKOSMAS: Is the following required? - REMOVED it! 10 JULY, 2020
    // if yes, for paraller receive buffers, shouldn't it be:
    // index->fbl = initialize_pRecBuf(index->settings->initial_fbl_buffer_size,
    //                             pow(2, index->settings->paa_segments),
    //                             index->settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);
    // index->fbl = initialize_fbl(settings->initial_fbl_buffer_size,
    //                             pow(2, settings->paa_segments),
    //                             settings->max_total_buffer_size+DISK_BUFFER_SIZE*(PROGRESS_CALCULATE_THREAD_NUMBER-1), index);


    // EKOSMAS: REMOVED 10 JULY, 2020
    // index->sax_cache = NULL;

    // EKOSMAS: REMOVED 10 JULY, 2020
    // index->total_records = 0;
    // index->loaded_records = 0;

    // EKOSMAS: REMOVED 10 JULY, 2020
    // index->root_nodes = 0;
    // index->allocated_memory = 0;
    // index->has_wedges = 0;

    // EKOSMAS: REMOVED 10 JULY, 2020
    // index->answer = malloc(sizeof(ts_type) * settings->timeseries_size);
    return index;
}


/**
 This function destroys an index.
 @param isax_index *index
 @param isax_ndoe *node
 */
void isax_index_destroy(isax_index *index, isax_node *node)
{
    if (node == NULL) {
        free(index->answer);
    	free(index->settings->bit_masks);
    	free(index->settings->raw_filename);
    	free(index->settings->max_sax_cardinalities);
    	free(index->settings);

		// TODO: OPTIMIZE TO FLUSH WITHOUT TRAVERSAL!
        isax_node *subtree_root = index->first_node;

        while (subtree_root != NULL)
        {
            isax_node *next =  subtree_root->next;
            isax_index_destroy(index, subtree_root);
            subtree_root = next;
        }
        destroy_fbl(index->fbl);
		#ifdef CLUSTERED
			free(index->locations);
		#endif
        if (index->sax_file !=NULL)
        {
            fclose(index->sax_file);
        }
        if(index->sax_cache != NULL)
            free(index->sax_cache);
        free(index);
    }
    else {
        // Traverse tree
        if(!node->is_leaf) {
            isax_index_destroy(index, node->right_child);
            isax_index_destroy(index, node->left_child);
        }

        if(node->split_data != NULL)
        {
            free(node->split_data->split_mask);
            free(node->split_data);
        }
        if(node->filename != NULL)
        {
            free(node->filename);
        }
        if(node->isax_cardinalities != NULL)
        {
            free(node->isax_cardinalities);
        }
        if(node->isax_values != NULL)
        {
            free(node->isax_values);
        }
        if(node->buffer != NULL)
        {
            destroy_node_buffer(node->buffer);
        }
        free(node);
    }
}
void isax_index_pRecBuf_destroy(isax_index *index, isax_node *node,int prewokernumber)
{
    EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : START --- \n\n");

    if (node == NULL) {

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL --- \n\n");

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE answer --- \n\n");
        free(index->answer);
        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE bit_masks --- \n\n");
        free(index->settings->bit_masks);
        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE raw_filename --- \n\n");
        if (index->settings->raw_filename == NULL) 
            {EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE raw_filename : IT IS NULL!!!! --- \n\n");}
        else {EKOSMAS_PRINT(("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE raw_filename : IT IS NOT NULL [%u]-->[%s] --- \n\n",index->settings->raw_filename, index->settings->raw_filename));}
        free(index->settings->raw_filename);
        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE max_sax_cardinalities --- \n\n");
        free(index->settings->max_sax_cardinalities);
        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FREE settings --- \n\n");
        free(index->settings);

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : FIRST SET OF FREE COMPLETED --- \n\n");

        // TODO: OPTIMIZE TO FLUSH WITHOUT TRAVERSAL!
        isax_node *subtree_root = index->first_node;

        while (subtree_root != NULL)
        {
            isax_node *next =  subtree_root->next;
            isax_index_destroy(index, subtree_root);
            subtree_root = next;
        }

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : TREE IS FREE --- \n\n");

        destroy_pRecBuf((parallel_first_buffer_layer*)(index->fbl),prewokernumber);

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : pRecBuf DESTROYED --- \n\n");

        #ifdef CLUSTERED
            free(index->locations);
        #endif
        if (index->sax_file !=NULL)
        {
            EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : sax_file - CLOSING --- \n\n");

            fclose(index->sax_file);

            EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : sax_file - CLOSED --- \n\n");
        }

        if(index->sax_cache != NULL) {
            EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : sax_cache FREE : START --- \n\n");
            free(index->sax_cache);
            EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : sax_cache FREE : FINISHED --- \n\n");
        }
        free(index);

        EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : NODE IS NULL : index is FREE --- \n\n");
    }
    else {
        // Traverse tree
        if(!node->is_leaf) {
            // EKOSMAS : change this with a call to isax_index_pRecBuf_destroy ???
            isax_index_destroy(index, node->right_child);
            // EKOSMAS : change this with a call to isax_index_pRecBuf_destroy ???
            isax_index_destroy(index, node->left_child);
        }

        if(node->split_data != NULL)
        {
            free(node->split_data->split_mask);
            free(node->split_data);
        }
        if(node->filename != NULL)
        {
            free(node->filename);
        }
        if(node->isax_cardinalities != NULL)
        {
            free(node->isax_cardinalities);
        }
        if(node->isax_values != NULL)
        {
            free(node->isax_values);
        }
        if(node->buffer != NULL)
        {
            destroy_node_buffer(node->buffer);
        }

        free(node);
    }

    EKOSMAS_PRINT("\n --- EKOSMAS : ADS : DOES NOT USE INDEX : DESTROY pRecBuf : END --- \n\n"); fflush(stdout);
}

void isax_tree_destroy(isax_node *node)
{
    // Traverse tree
    if(!node->is_leaf) {
        isax_tree_destroy(node->right_child);
        isax_tree_destroy(node->left_child);
    }
    if(node->split_data != NULL)
    {
        free(node->split_data->split_mask);
        free(node->split_data);
    }
    if(node->filename != NULL)
    {
        free(node->filename);
    }
    if(node->isax_cardinalities != NULL)
    {
        free(node->isax_cardinalities);
    }
    if(node->isax_values != NULL)
    {
        free(node->isax_values);
    }
    if(node->buffer != NULL)
    {
        destroy_node_buffer(node->buffer);
    }

    free(node);
}


void isax_tree_destroy_lockfree(isax_node *node)
{
    if(!node){
        return;
    }
    // Traverse tree
    
    if(!node->is_leaf) {
        isax_tree_destroy_lockfree(node->right_child);
        isax_tree_destroy_lockfree(node->left_child);
    }

    if(node->split_data != NULL)
    {
        free(node->split_data->split_mask);
        free(node->split_data);
    }
    if(node->buffer != NULL)
    {
        destroy_node_buffer(node->buffer);
    }
    if (node->announce_array != NULL) {
        free((void *)node->announce_array);
    }
   /* if(node->reference_counters != NULL){
        free(node->reference_counters);
    }
    */ //Error double free here.

    free(node);
}

void isax_tree_destroy_lockfree_geopat(isax_node_single_buffer *node)
{
    // Traverse tree
    if(!node->node->is_leaf) {
        isax_tree_destroy_lockfree_geopat(node->node->right_child);
        isax_tree_destroy_lockfree_geopat(node->node->left_child);
    }

    if(node->node->split_data != NULL)
    {
        free(node->node->split_data->split_mask);
        free(node->node->split_data);
    }
    if(node->node->buffer != NULL)
    {
        destroy_node_buffer(node->node->buffer);
    }
    if (node->node->announce_array != NULL) {
        free((void *)node->node->announce_array);
    }
   /* if(node->reference_counters != NULL){
        free(node->reference_counters);
    }
    */ //Error double free here.

    free(node);
}

enum response create_node_filename(isax_index *index,
                                   isax_node *node,
                                   isax_node_record *record)
{
    int i;

    node->filename = malloc(sizeof(char) * index->settings->max_filename_size);
    sprintf(node->filename, "%s", index->settings->root_directory);
    int l = (int) strlen(index->settings->root_directory);

    // If this has a parent then it is not a root node and as such it does have some
    // split data on its parent about the cardinalities.
    node->isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments);
    node->isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);

    if (node->parent) {
        for (i=0; i<index->settings->paa_segments; i++) {
            root_mask_type mask = 0x00;
            int k;
            for (k=0; k <= ((isax_node*)node->parent)->split_data->split_mask[i]; k++)
            {
                mask |= (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1 - k] &
                         record->sax[i]);
            }
            mask = mask >> index->settings->sax_bit_cardinality - ((isax_node*)node->parent)->split_data->split_mask[i] - 1;

            node->isax_values[i] = (int) mask;
            node->isax_cardinalities[i] = ((isax_node*)node->parent)->split_data->split_mask[i]+1;

            if (i==0) {
                l += sprintf(node->filename+l ,"%d.%d", node->isax_values[i], node->isax_cardinalities[i]);
            }
            else {
                l += sprintf(node->filename+l ,"_%d.%d", node->isax_values[i], node->isax_cardinalities[i]);
            }

        }
    }
    // If it has no parent it is root node and as such it's cardinality is 1.
    else
    {
        root_mask_type mask = 0x00;

        for (i=0; i<index->settings->paa_segments; i++) {

            mask = (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & record->sax[i]);
            mask = mask >> index->settings->sax_bit_cardinality - 1;

            node->isax_values[i] = (int) mask;
            node->isax_cardinalities[i] = 1;

            if (i==0) {
                l += sprintf(node->filename+l ,"%d.1", (int) mask);
            }
            else {
                l += sprintf(node->filename+l ,"_%d.1", (int) mask);
            }
        }
    }

#ifdef DEBUG
    printf("\tCreated filename:\t\t %s\n\n", node->filename);
#endif

    return SUCCESS;
}

inline enum response initialize_isax_values_and_cardinalities(isax_index *index,
                                   isax_node *node,
                                   sax_type *sax)
{
    int i;
    node->isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments);
    node->isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);

    // If this has a parent then it is not a root node and as such it does have some
    // split data on its parent about the cardinalities.
    if (node->parent) {
        for (i=0; i<index->settings->paa_segments; i++) {
            root_mask_type mask = 0x00;
            int k;
            for (k=0; k <= ((isax_node*)node->parent)->split_data->split_mask[i]; k++)
            {
                mask |= (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1 - k] & sax[i]);
            }
            mask = mask >> index->settings->sax_bit_cardinality - ((isax_node*)node->parent)->split_data->split_mask[i] - 1;

            node->isax_values[i] = (int) mask;
            node->isax_cardinalities[i] = ((isax_node*)node->parent)->split_data->split_mask[i]+1;
        }
    }
    // If it has no parent it is root node and as such it's cardinality is 1.
    else
    {
        root_mask_type mask = 0x00;

        for (i=0; i<index->settings->paa_segments; i++) {

            mask = (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax[i]);
            mask = mask >> index->settings->sax_bit_cardinality - 1;

            node->isax_values[i] = (int) mask;
            node->isax_cardinalities[i] = 1;
        }
    }

    return SUCCESS;
}

inline enum response initialize_isax_values_and_cardinalities_lockfree(isax_index *index,
                                   isax_node *node,
                                   sax_type *sax)
{
    int i;
    sax_type *isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments); 
    sax_type *isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);
    // node->isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments);
    // node->isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);

    // If this has a parent then it is not a root node and as such it does have some
    // split data on its parent about the cardinalities.
    if (node->parent) {
        for (i=0; i<index->settings->paa_segments; i++) {
            root_mask_type mask = 0x00;
            int k;
            for (k=0; k <= ((isax_node*)node->parent)->split_data->split_mask[i]; k++)
            {
                mask |= (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1 - k] & sax[i]);
            }
            mask = mask >> index->settings->sax_bit_cardinality - ((isax_node*)node->parent)->split_data->split_mask[i] - 1;

            isax_values[i] = (int) mask;
            isax_cardinalities[i] = ((isax_node*)node->parent)->split_data->split_mask[i]+1;
        }
    }
    // If it has no parent it is root node and as such it's cardinality is 1.
    else
    {
        root_mask_type mask = 0x00;

        for (i=0; i<index->settings->paa_segments; i++) {

            mask = (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax[i]);
            mask = mask >> index->settings->sax_bit_cardinality - 1;

            isax_values[i] = (int) mask;
            isax_cardinalities[i] = 1;
        }
    }


    if (node->isax_cardinalities == NULL) {
        CASPTR(&(node->isax_cardinalities), NULL, isax_cardinalities);
    }

    if (node->isax_values == NULL) {
        CASPTR(&(node->isax_values), NULL, isax_values);
    }

    return SUCCESS;
}

inline enum response initialize_isax_values_and_cardinalities_lockfree_single_buffer(isax_index *index,
                                   isax_node_single_buffer *node,
                                   sax_type *sax)
{
    int i;
    sax_type *isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments); 
    sax_type *isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);
    // node->isax_values = malloc(sizeof(sax_type) * index->settings->paa_segments);
    // node->isax_cardinalities = malloc(sizeof(sax_type) * index->settings->paa_segments);

    // If this has a parent then it is not a root node and as such it does have some
    // split data on its parent about the cardinalities.
    if (node->node->parent) {
        for (i=0; i<index->settings->paa_segments; i++) {
            root_mask_type mask = 0x00;
            int k;
            for (k=0; k <= ((isax_node_single_buffer*)node->node->parent)->node->split_data->split_mask[i]; k++)
            {
                mask |= (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1 - k] & sax[i]);
            }
            mask = mask >> index->settings->sax_bit_cardinality - ((isax_node_single_buffer*)node->node->parent)->node->split_data->split_mask[i] - 1;

            isax_values[i] = (int) mask;
            isax_cardinalities[i] = ((isax_node_single_buffer*)node->node->parent)->node->split_data->split_mask[i]+1;
        }
    }
    // If it has no parent it is root node and as such it's cardinality is 1.
    else
    {
        root_mask_type mask = 0x00;

        for (i=0; i<index->settings->paa_segments; i++) {

            mask = (index->settings->bit_masks[index->settings->sax_bit_cardinality - 1] & sax[i]);
            mask = mask >> index->settings->sax_bit_cardinality - 1;

            isax_values[i] = (int) mask;
            isax_cardinalities[i] = 1;
        }
    }


    if (node->node->isax_cardinalities == NULL) {
        CASPTR(&(node->node->isax_cardinalities), NULL, isax_cardinalities);
    }

    if (node->node->isax_values == NULL) {
        CASPTR(&(node->node->isax_values), NULL, isax_values);
    }

    return SUCCESS;
}

isax_node * add_record_to_node_inmemory(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record,
                                 const char leaf_size_check)
{
    #ifdef DEBUG
    printf("*** Adding to node ***\n\n");
    #endif
    isax_node *node = tree_node;

    // Traverse tree
    while (!node->is_leaf) {
        int location = index->settings->sax_bit_cardinality - 1 -
        node->split_data->split_mask[node->split_data->splitpoint];

        root_mask_type mask = index->settings->bit_masks[location];
        if(record->sax[node->split_data->splitpoint] & mask)
        {
            node = node->right_child;
        }
        else
        {
            node = node->left_child;
        }
    }
    // Check if split needed
    if ((node->leaf_size) >= index->settings->max_leaf_size && leaf_size_check) {
    #ifdef DEBUG
        printf(">>> %s leaf size: %d\n\n", node->filename, node->leaf_size);
    #endif
        split_node_inmemory(index, node);
        // add_record_to_node(index, node, record, leaf_size_check);
        add_record_to_node_inmemory(index, node, record, leaf_size_check);          // EKOSMAS: CHANGED 07 JULY 2020
    }
    else
    {
        if (node->isax_values == NULL) {                                            // EKOSMAS: ADDED 30 AUGUST 2020
            initialize_isax_values_and_cardinalities(index, node, record->sax);
        }
        add_to_node_buffer(node->buffer, record, index->settings->paa_segments,
                           index->settings->timeseries_size);
        node->leaf_size++;

    }
    return node;
}

static inline isax_node * traverse_to_next_node (isax_index *index, isax_node *node, isax_node_record *record) {
    int location = index->settings->sax_bit_cardinality - 1 -
    node->split_data->split_mask[node->split_data->splitpoint];

    root_mask_type mask = index->settings->bit_masks[location];
    if(record->sax[node->split_data->splitpoint] & mask)
    {
        node = node->right_child;
    }
    else
    {
        node = node->left_child;
    }
    
    return node;   
}

static inline isax_node_single_buffer * traverse_to_next_node_single_buffer (isax_index *index, isax_node_single_buffer *node, isax_node_record *record) {
    int location = index->settings->sax_bit_cardinality - 1 -
    node->node->split_data->split_mask[node->node->split_data->splitpoint];

    root_mask_type mask = index->settings->bit_masks[location];
    if(record->sax[node->node->split_data->splitpoint] & mask)
    {
        node = (isax_node_single_buffer*) node->node->right_child;
    }
    else
    {
        node = (isax_node_single_buffer*) node->node->left_child;
    }
    
    return node;   
}

isax_node * add_record_to_node_inmemory_parallel_locks(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record)
{
    #ifdef DEBUG
    printf("*** Adding to node ***\n\n");
    #endif
    isax_node *node = tree_node;

    while (1) {
        // Traverse tree
        while (!node->is_leaf) {
            node = traverse_to_next_node(index, node, record);
        }

        pthread_mutex_lock(node->lock_node);

        if (!node->is_leaf) {
            pthread_mutex_unlock(node->lock_node);
            continue;
        }

        // Check if split needed
        if ((node->leaf_size) >= index->settings->max_leaf_size) {
            split_node_inmemory_parallel_locks(index, node);
            pthread_mutex_unlock(node->lock_node);
            continue;
        }

        if (node->isax_values == NULL) {                                            // EKOSMAS: ADDED 02 SEPTEMBER 2020
            initialize_isax_values_and_cardinalities(index, node, record->sax);
        }

        add_to_node_buffer(node->buffer, record, index->settings->paa_segments,
                           index->settings->timeseries_size);

        node->leaf_size++;

        pthread_mutex_unlock(node->lock_node);

        return node;
    }
}

isax_node * add_record_to_node_inmemory_parallel_lockfree_announce(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id,
                                 unsigned long total_workers_num,
                                 const char is_helper,
                                 const char lockfree_parallelism_in_subtree)
{
    #ifdef DEBUG
    printf("*** Adding to node ***\n\n");
    #endif
    isax_node *node = current_fbl_node->node;
    unsigned long next_buf_pos;
    announce_rec *new_announce_rec;
    unsigned char new_ann_rec_flag = 1;

    while (1) {
        // Traverse tree
        while (!node->is_leaf) {
            node = traverse_to_next_node(index, node, record);
        }

        // Take next free position in buffer
        unsigned char lightweight_path = 0;                 // follow "heavy path", when i) alorithm announces independently of helpers, or ii) algorithm announces after helpers and helpers exist (per subtree or per leaf)
        if ((is_helper || (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist)) ||
            (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && current_fbl_node->recBuf_helpers_exist) || 
            lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {

            if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && is_helper && !node->recBuf_leaf_helpers_exist) {
                node->recBuf_leaf_helpers_exist = 1;
            }

            if (node->announce_array == NULL) {                                     // EKOSMAS AUGUST 09, 2020: IS THERE A BETTER POSITION FOR THIS???
                announce_rec * volatile *tmp_announce_array = calloc(total_workers_num, sizeof(announce_rec *));
                if(!CASPTR(&(node->announce_array), NULL, tmp_announce_array)) {
                    free((void *)tmp_announce_array);
                }
            }
            
            if (new_ann_rec_flag) {
                new_announce_rec = create_new_announce_rec(record);
                if (node->announce_array == NULL) {printf("ERROR: NULL node->announce_array -1- !!!!"); fflush(stdout);}
                node->announce_array[my_id] = new_announce_rec;
            }
            else {
                new_ann_rec_flag = 1;
            }
            if (node->fai_leaf_size == 0) {                                         // initialize fai_leaf_size
                unsigned long cur_size = node->leaf_size;
                if (cur_size) {
                    CASULONG(&(node->fai_leaf_size), 0, cur_size);      
                }
            }
            next_buf_pos = __sync_fetch_and_add(&(node->fai_leaf_size),1);
        }
        else {                                                                      // algorithm announces after helpers, but no helpers exist; so, follow "lightweight path"
            next_buf_pos = node->leaf_size++;
            lightweight_path = 1;                                                   // note that lightweight path has been selected
        }     

        // Check if split needed
        if (next_buf_pos >= index->settings->max_leaf_size) {
            isax_node *tmp_node = node;
            node = split_node_inmemory_parallel_lockfree_announce(index, node, current_fbl_node, total_workers_num, my_id, lockfree_parallelism_in_subtree, lightweight_path, 0);

            if (node && node->announce_array && node->announce_array[my_id] && node->announce_array[my_id]->record.sax == record->sax && node->announce_array[my_id]->buf_pos != ULONG_MAX) {
                node->announce_array[my_id] = NULL;
                return node;
            }
            else if (!node) {                       // it has followed lightweight path, but a helper appeared!
                node = tmp_node->parent;
                if (!node) {
                    node = current_fbl_node->node;
                }
            }
            else if (lightweight_path == 0) {       // if it has followed heavy path, then it has already announced its record insertion using an announce record, which has allocated, so do not allocate a new announce record
                new_ann_rec_flag = 0;
            }

            continue;
        }

        // update announce with position "next_buf_pos"
        if ((lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist) ||
            (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && current_fbl_node->recBuf_helpers_exist) ||
            lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
            if (lightweight_path == 1) {                                            // if lightweight path has been followed then return back, since you have to follow heavy path
                node = node->parent;
                if (!node) {
                    node = current_fbl_node->node;
                }
                continue;
            }
            new_announce_rec->buf_pos = next_buf_pos;
        }

        // add record to buffer
        add_to_node_buffer_lockfree(node->buffer, record, next_buf_pos);

        // if (node->isax_values == NULL &&  node->buffer->partial_sax_buffer[0] != NULL) {                                 // EKOSMAS: ADDED 30 AUGUST 2020
        //     initialize_isax_values_and_cardinalities_lockfree(index, node, node->buffer->partial_sax_buffer[0]);
        if (node->isax_values == NULL) {                                                                                    // EKOSMAS: CHANGED 01 SEPTEMBER 2020
            initialize_isax_values_and_cardinalities_lockfree(index, node, record->sax);
        }        

        // remove announcement
        if ((lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP_PER_LEAF && node->recBuf_leaf_helpers_exist) ||
            (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && current_fbl_node->recBuf_helpers_exist) ||
            lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
            if (lightweight_path == 1) {                                            // if lightweight path has been followed then return back, since you have to follow heavy path, in order to ensure that your record has been added. At the end you may re-enter your record (so, it may appear twice in the index)
                node = node->parent;
                if (!node) {
                    node = current_fbl_node->node;
                }
                continue;
            }
            if (node->announce_array) {
                node->announce_array[my_id] = NULL;
            }
        }

        return node;
    }
}

isax_node *add_record_to_node_inmemory_parallel_lockfree_announce_local(isax_index *index,
                                 isax_node *tree_node,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long total_workers_num,
                                 unsigned long my_id,
                                 const char lockfree_parallelism_in_subtree,
                                 unsigned char lightweight_path)
{
    isax_node *node = tree_node;
    unsigned long next_buf_pos;

    while (1) {
        // Traverse tree
        while (!node->is_leaf) {
            node = traverse_to_next_node(index, node, record);
        }

        // if (lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE || current_fbl_node->recBuf_helpers_exist) {
        if ((lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE_AFTER_HELP && current_fbl_node->recBuf_helpers_exist) ||
            lockfree_parallelism_in_subtree == LOCKFREE_PARALLELISM_IN_SUBTREE_ANNOUNCE) {
            if (lightweight_path == 1) {            // lightweight path should change to heavy path
                return NULL;
            }
            next_buf_pos = node->fai_leaf_size++;
        }
        else {
            next_buf_pos = node->leaf_size++;
        }

        // Check if split needed
        if (next_buf_pos >= index->settings->max_leaf_size) {
            node = split_node_inmemory_parallel_lockfree_announce(index, node, current_fbl_node, total_workers_num, my_id, lockfree_parallelism_in_subtree, lightweight_path, 1);
            if (!node) {                            // lightweight path should change to heavy path
                return NULL;
            }
            continue;
        }

        add_to_node_buffer_lockfree(node->buffer, record, next_buf_pos);

        if (node->isax_values == NULL) {                                            // EKOSMAS: ADDED 30 AUGUST 2020
            initialize_isax_values_and_cardinalities(index, node, node->buffer->partial_sax_buffer[0]);
        }

        return node;
    }
}

isax_node * add_record_to_node_inmemory_parallel_lockfree_cow(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id)
{
    #ifdef DEBUG
    printf("*** Adding to node ***\n\n");
    #endif
    isax_node *node = current_fbl_node->node;

    while (1) {
        // Traverse tree
        while (!node->is_leaf) {
            node = traverse_to_next_node(index, node, record);
        }
        // Read node->buffer
       isax_node_buffer *tmp_buffer = node->buffer;
            // Check if split needed
            if (tmp_buffer->partial_buffer_size == index->settings->max_leaf_size) {
                node = split_node_inmemory_parallel_lockfree_cow(index, node, current_fbl_node, my_id);
                continue;

                // Another solution...
                // split_node_inmemory_parallel_lockfree_cow(index, node, current_fbl_node, my_id);
                // add_record_to_node_inmemory_parallel_lockfree_cow(index, current_fbl_node, r, my_id);
            }

            // Clone existing buffer with +1 size
            isax_node_buffer *new_buffer = clone_buffer_and_add_record_lockfree_cow(tmp_buffer, record);
            destroy_node_buffer(tmp_buffer);

            // try to atomically etablish new buffer
            if(CASPTR(&node->buffer, tmp_buffer, new_buffer)){
                if (node->isax_values == NULL) {                                                        // EKOSMAS: ADDED 02 SEPTEMBER 2020
                    initialize_isax_values_and_cardinalities(index, node, node->buffer->partial_sax_buffer[0]);
                }
                return node;    
            }
            else {
                destroy_node_buffer(new_buffer);            // alternatively, we could use realloc and reuse allocated memory to achieve better(?) performance. I can experiment with this.
                if (node->parent == NULL) {
                    node = current_fbl_node->node;
                }
                else {
                    node = node->parent;   
                }
            }
    }   
}



inline isax_node_single_buffer * search(isax_index *index, isax_node_single_buffer *node,isax_node_record* record){
     while (node->node->is_leaf != 1) {
            node = traverse_to_next_node_single_buffer(index, node, record);
    }

    return node;
}


 int insertIsaxToTree(isax_index *index,parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,unsigned long my_id,InfoRecord * op,int rec_buff_exists,int helper){
    
    //Reading data from Info Record
    isax_node_single_buffer *leaf = op->leaf;
    isax_node_buffer *tmp_buffer  = leaf->node->buffer;
    isax_node_record *record = op->record;
    long position = op->position;

    int split_done = 0;
    Update *parent_update;
    if((isax_node_single_buffer*)leaf->node->parent){
        parent_update = ((isax_node_single_buffer*)leaf->node->parent)->update; //Read Parentt update
    }
    else{
        parent_update = current_fbl_node->rootSubtreesUpdateArray[(int)*record->mask];
    }
  
    if(position == 2000){ //kanoume split
        leaf = split_node_inmemory_parallel_lockfree_cow_geopat(index, leaf, current_fbl_node, my_id,*record->mask , 1);
        split_done = 1;
    }
    else{
        if(op->leaf->node->buffer->partial_buffer_size == position){
                leaf->node->buffer->partial_sax_buffer[position] = record->sax;
                leaf->node->buffer->partial_position_buffer[position] = record->position;
                if(leaf->node->isax_values == NULL){
                    initialize_isax_values_and_cardinalities_lockfree_single_buffer(index,leaf,record->sax);
                }
                CASPTR(&op->leaf->node->buffer->partial_buffer_size,position,position+1);
                if(rec_buff_exists == 1){
                    if(current_fbl_node->iSAX_processed[op->fbl_position] == 0){
                        current_fbl_node->iSAX_processed[op->fbl_position] = 1;
                    }
                }   
        }      
    }                      

    //IUNFLAG
    if(op->leaf->node->parent != NULL){  
        op->parent->update->state = CLEAN;
    }
    else{
        current_fbl_node->rootSubtreesUpdateArray[*record->mask]->state = CLEAN;
    }

    return split_done;
     
}


//Similar to
inline void help_tree_insert(InfoRecord *info , parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node, isax_index *index ,unsigned long my_id,int rec_buff_exists){
    if(info == NULL){
        return ; 
    }
    else{
        insertIsaxToTree(index,current_fbl_node,my_id,info,rec_buff_exists,1);
    }
   
}


isax_node_single_buffer *  add_record_to_node_inmemory_parallel_lockfree_geopat(isax_index *index,
                                 parallel_fbl_soft_buffer_ekosmas_lf_geopat *current_fbl_node,
                                 isax_node_record *record,
                                 unsigned long my_id,root_mask_type mask,unsigned long current_pos,int rec_buff_exists)
{
    
    isax_node_single_buffer *node = current_fbl_node->node[(unsigned long )mask];

    while (1) {

        node = search(index, node, record);
        Update *parent_update;

        if((isax_node_single_buffer*)node->node->parent != NULL){
            parent_update = ((isax_node_single_buffer*)node->node->parent)->update; //Read Parentt update
        }
        else{
            parent_update = current_fbl_node->rootSubtreesUpdateArray[(unsigned long)mask];
        }

        if(parent_update->state != CLEAN){
            if(node->node->parent != NULL){
                help_tree_insert(((isax_node_single_buffer*)(node->node->parent))->update->info,current_fbl_node,index,my_id,rec_buff_exists);
            }
            else{
                help_tree_insert(current_fbl_node->rootSubtreesUpdateArray[(unsigned long)mask]->info,current_fbl_node,index,my_id,rec_buff_exists);
            }
        }
        else{ //If is Clean
            Update* new_update = malloc(sizeof(Update)) ;
            //New Info Record
            InfoRecord *newInfo = malloc(sizeof(InfoRecord));
            newInfo->leaf = node;
            newInfo->position = node->node->buffer->partial_buffer_size;
            newInfo->parent = (isax_node_single_buffer*)node->node->parent;
            newInfo->record = record;
            newInfo->fbl_position = current_pos;
            //New update
            new_update->state = IFLAG;
            new_update->info = newInfo;
            if(node->node->parent != NULL){
                if(CASPTR(&((isax_node_single_buffer*)node->node->parent)->update,parent_update,new_update)){ //IFLAG CAS SUCCESSFULL
                        int result = insertIsaxToTree(index,current_fbl_node,my_id,new_update->info,rec_buff_exists,0);
                        if(result == 0){
                           return node;
                        }           
                }
                else{ //Someone else established first his insert  So help him
                    help_tree_insert(((isax_node_single_buffer*)node->node->parent)->update->info,current_fbl_node,index,my_id,rec_buff_exists);
                }
            }
            else{
                if(CASPTR(&current_fbl_node->rootSubtreesUpdateArray[(unsigned long)mask],parent_update,new_update)){ //IFLAG CAS SUCCESSFULL
                        int result = insertIsaxToTree(index,current_fbl_node,my_id,new_update->info,rec_buff_exists,0);
                        if(result == 0){
                            break;
                        }
                    return node;

                }
                else{ //Someone else established first his insert  So help him
                    help_tree_insert(current_fbl_node->rootSubtreesUpdateArray[(unsigned long)mask]->info,current_fbl_node,index,my_id,rec_buff_exists);
                }
            }
                   
        }

        node = current_fbl_node->node[(unsigned long)mask]; // Begin From the start again maybe Split happend
    }   
}



// isax_node *add_record_to_node_inmemory_parallel_lockfree_cow_local(isax_index *index,
void add_record_to_node_inmemory_parallel_lockfree_cow_local(isax_index *index,
                                 isax_node *tree_node,
                                 isax_node_record *record,
                                 unsigned long my_id)
{
    isax_node *node = tree_node;

    // while (1) {
    //     // Traverse tree
    //     while (!node->is_leaf) {
    //         node = traverse_to_next_node(index, node, record);
    //     }

        // // Check if split needed
        // if (next_buf_pos >= index->settings->max_leaf_size) {
        //     // printf("Thread [%d]: Populating SubTree (Lock Free) LOCALLY - START - adding iSAX - START - split - START\n", my_id);fflush(stdout);
        //     split_node_inmemory_parallel_lockfree_cow(index, node, NULL, my_id);                    // EKOSMAS: Can this NULL make any harm???
        //     // printf("Thread [%d]: Populating SubTree (Lock Free) LOCALLY - START - adding iSAX - START - split - END\n", my_id);fflush(stdout);
        //     continue;
        // }

        add_to_node_buffer_lockfree(node->buffer, record, node->buffer->partial_buffer_size);
        node->buffer->partial_buffer_size++;

        if (node->isax_values == NULL) {                                                                    // EKOSMAS: ADDED 02 SEPTEMBER 2020
            initialize_isax_values_and_cardinalities(index, node, node->buffer->partial_sax_buffer[0]);
        }

        // return node;
    // }
}

void add_record_to_node_inmemory_parallel_lockfree_cow_local_geopat(isax_index *index,
                                 isax_node_single_buffer *tree_node,
                                 isax_node_record *record,
                                 unsigned long my_id,int function_type)
{
    isax_node_single_buffer *node = tree_node;

    add_to_node_buffer_lockfree_geopat(node->node->buffer, record, node->node->buffer->partial_buffer_size);
    node->node->buffer->partial_buffer_size++;

    if (node->node->isax_values == NULL) {                                                                    // EKOSMAS: ADDED 02 SEPTEMBER 2020
        initialize_isax_values_and_cardinalities_lockfree_single_buffer(index, node, node->node->buffer->partial_sax_buffer[0]);
    }

}



void print_settings(isax_index_settings *settings) {
	fprintf(stderr,"############ ParIS SETTINGS ############\n");
	fprintf(stderr,"## [FILE SETTINGS]\n");
	fprintf(stderr,"## raw_filename:\t%s\n",settings->raw_filename);
	fprintf(stderr,"## root_directory:\t%s\n",settings->root_directory);

	fprintf(stderr,"## \n## [DATA TYPE SETTINGS]\n");
	fprintf(stderr,"## timeseries_size:\t%d\n",settings->timeseries_size);
	fprintf(stderr,"## partial_record_size:\t%d\n",settings->partial_record_size);
	fprintf(stderr,"## full_record_size:\t%d\n",settings->full_record_size);
	fprintf(stderr,"## position_byte_size:\t%d\n",settings->position_byte_size);
	fprintf(stderr,"## sax_byte_size:\t%d\n",settings->sax_byte_size);
	fprintf(stderr,"## ts_byte_size:\t%d\n",settings->ts_byte_size);


	fprintf(stderr,"## \n## [BUFFER SETTINGS]\n");
	fprintf(stderr, "## initial_fbl_buffer_size:\t%d\n", settings->initial_fbl_buffer_size);
	fprintf(stderr, "## initial_leaf_buffer_size:\t%d\n", settings->initial_leaf_buffer_size);
	fprintf(stderr,"## max_total_buffer_size:\t%d\n",settings->max_total_buffer_size);
	fprintf(stderr,"## max_total_full_buffer_size:\t%d\n",settings->max_total_full_buffer_size);

	fprintf(stderr,"## \n## [LEAF SETTINGS]\n");
	fprintf(stderr, "## max_leaf_size:\t%d\n", settings->max_leaf_size);
	fprintf(stderr, "## min_leaf_size:\t%d\n",settings->min_leaf_size);

	fprintf(stderr,"## \n## [SAX SETTINGS]\n");
	fprintf(stderr,"## paa_segments:\t%d\n",settings->paa_segments);
	fprintf(stderr,"## sax_alphabet_card.:\t%d\n",settings->sax_alphabet_cardinality);
	fprintf(stderr,"## sax_bit_cardinality:\t%d\n",settings->sax_bit_cardinality);

	fprintf(stderr,"## \n## [QUERY ANSWERING SETTINGS]\n");
	fprintf(stderr, "## aggressive_check:\t%d\n", settings->aggressive_check);
	fprintf(stderr,"## tight_bound:\t%d\n",settings->tight_bound);
	fprintf(stderr,"## total_loaded_leaves:\t%d\n",settings->total_loaded_leaves);
    fprintf(stderr,"######################################\n");

	fflush(stderr);
}
