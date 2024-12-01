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
// skipSearch_node* getskipSearchNode(isax_node*node,attribute_type* attribute){
//     return NULL;
//     if(node->skipSearch_list==NULL)return NULL;

//     if(*node->skipSearch_list->attribute == *attribute){return node->skipSearch_list;
//     }else{ return NULL;}


//     skipSearch_node * temp = node->skipSearch_list;
//     while(temp != NULL){
//         if(*attribute == *temp->attribute)return temp;
//         temp=temp->next;
//     }
//     return NULL;
// }
int areAttributesEqual(attribute_type* attribute1,attribute_type* attribute2,int attribute_size){
    if(attribute1==NULL||attribute2==NULL){printf("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n");return 0;}
    for(int i =0; i<attribute_size; i++){
        if(attribute1[i]!=attribute2[i]){
            return 0;
        }
    }
    return 1;
}
int leafContainsAttr2(isax_node* root , attribute_type* attribute){
    // if(*attribute == 63){
         
            
    //             //  for(int i = 0; i<root->buffer->partial_buffer_size;i++){
    //             //      printf(" %ld",*root->buffer->partial_attribute_buffer[i]);
    //             //  }
    //             // printf("\n");
    //             for(int i = 0; i<100;i++)printf(" %d/%d",*root->buffer->partial_attribute_buffer[0],i);
        
    // }
    for(int i = 0; i<root->buffer->partial_buffer_size;i++){
        if(areAttributesEqual(attribute,root->buffer->partial_attribute_buffer[i],3)){
            return 1;
        }
    }
    return 0;
}
int printAttr(isax_node* root , attribute_type* attribute){
    // if(*attribute == 63){
         
            
    //             //  for(int i = 0; i<root->buffer->partial_buffer_size;i++){
    //             //      printf(" %ld",*root->buffer->partial_attribute_buffer[i]);
    //             //  }
    //             // printf("\n");
    //             for(int i = 0; i<100;i++)printf(" %d/%d",*root->buffer->partial_attribute_buffer[0],i);
        
    // }
    for(int i = 0; i<root->buffer->partial_buffer_size;i++){
            printf(" %d ",*root->buffer->partial_attribute_buffer[i],i);
    }

    //printf(" %d ",*root->buffer->partial_attribute_buffer[root->buffer->partial_buffer_size-1] );
    return 0;
}
// int evalSubtree(isax_node* root , attribute_type* attribute,isax_index* index){
    
//     if(root->is_leaf)return leafContainsAttr(root,attribute);
//     isax_node * temp = root->leftmost_leaf;
//     int max = root->rightmost_leaf->leaf_id;
//     //printf("starting from : %d to %d\n",temp->leaf_id,max);
//     while(temp!=NULL && temp->leaf_id <= max){
//         if(leafContainsAttr(temp,attribute))return 1;
//         temp = temp->leaflist_next;
//     }
//     return 0;
// //return 1;
// }
int leafContainsAttr(isax_node* root , attribute_type* attribute,isax_index*index){
    if(root == NULL )return 0;
    int dim = index->settings->attribute_size;
    float *rect_min = malloc(sizeof(float)*dim);
    float *rect_max = malloc(sizeof(float)*dim);
    for (int i = 0 ; i < dim; i++){
        rect_min[i] = attribute[i];
        rect_max[i] = attribute[i];
    }
    struct pqueue *result;
    if ((result = kd_ortRangeSearch(root->kdtree, rect_min, rect_max, 3)) == NULL) {
        fprintf(stderr, "Orthogonal range search failed.\n");
        exit(EXIT_FAILURE);
        return 0;
    }
    //printf(" r=%d ",result->size);
    int retval; 
    if(result->size>(unsigned int)1){retval= 1;}
    else{
      retval= 0;
    }



    for(int i = 1; i < result->size; i++) {

        free(result->d[i]);
    }
    free(result->d);
    free(result);
    free(rect_max);
    free(rect_min);
    return retval;
    // struct kdNode * kdnode=root->kdtree;
    // while(1){
    //     if(kdnode==NULL){return 0;}
    //     if(kdnode->location[0]==attribute[0]&&kdnode->location[1]==attribute[1]&&kdnode->location[2]==attribute[2]){
    //         //printf("found %f %f %f\n",kdnode->location[0],kdnode->location[1],kdnode->location[2]);
    //         return 1;
    //     }
    //     if(kdnode->left==NULL&&kdnode->right==NULL){
    //         return 0;
    //     }

    //     if(((float)attribute[kdnode->split]) < kdnode->location[kdnode->split]){
    //         kdnode = kdnode->left;
    //     }else{
    //         kdnode = kdnode->right;
    //     }
    // }


}
// int leafContainsAttr(isax_node* root , attribute_type* attribute){
//     // if(*attribute == 63){
         
            
//     //             //  for(int i = 0; i<root->buffer->partial_buffer_size;i++){
//     //             //      printf(" %ld",*root->buffer->partial_attribute_buffer[i]);
//     //             //  }
//     //             // printf("\n");
//     //             for(int i = 0; i<100;i++)printf(" %d/%d",*root->buffer->partial_attribute_buffer[0],i);
        
//     // }
//     for(int i = 0; i<root->buffer->partial_buffer_size;i++){
//         if((long int)(*root->buffer->partial_attribute_buffer[i])==(long int)(*attribute)){
            
//             return 1;
//         }
//     }
//     return 0;
// }

int evalSubtree(isax_node* root , attribute_type* attribute,isax_index*index){//TEST remove free
    if(root==NULL)return 0;
    if(root->is_leaf ){
        if(root->buffer->partial_buffer_size == 0)return 0;
        if(root->attribute_when_searchedFirst == NULL || root->supports_pred == 0 || areAttributesEqual(root->attribute_when_searchedFirst,attribute,index->settings->attribute_size)==0){//*root->attribute_when_searchedFirst != *attribute){//TO BE COMPARED PROPERLY

            int result = leafContainsAttr(root,attribute,index);
            if(result == 1){
                root->supports_pred = 1;
            }else{
                root->supports_pred = 2;
            }
            
            // root->attribute_when_searchedFirst = attribute;
            if(root->attribute_when_searchedFirst!=NULL){
            for(int i =0;i<index->settings->attribute_size;i++){
                root->attribute_when_searchedFirst[i]=attribute[i];
            }
            }else{
            root->attribute_when_searchedFirst = malloc(sizeof(attribute_type)*index->settings->attribute_size);
            for(int i =0;i<index->settings->attribute_size;i++){
                root->attribute_when_searchedFirst[i]=attribute[i];
            }
            }

            root->nextInSkiplist =NULL;
           // memcpy((root->attribute_when_searchedFirst),attribute,sizeof(attribute_type)*index->settings->attribute_size);//TEST same
    
            return result;
        }else{
            if(root->supports_pred==2){
                return 0;
            }else if(root->supports_pred == 1){
                return 1;
            }
        }
    }
    isax_node * temp = root->leftmost_leaf;
    int max = root->rightmost_leaf->leaf_id;
    //printf("starting from : %d to %d\n",temp->leaf_id,max);
    isax_node** history=malloc(sizeof(isax_node*)*(root->rightmost_leaf->leaf_id - root->leftmost_leaf->leaf_id + 2));
    int history_count=0;
    while(temp!=NULL && temp->leaf_id <= max){
        if(temp->attribute_when_searchedFirst == NULL || areAttributesEqual(temp->attribute_when_searchedFirst,attribute,index->settings->attribute_size)==0 || temp->supports_pred ==0){//TO BE COMPARED PROPERLY

            if(temp->attribute_when_searchedFirst!=NULL){
            for(int i =0;i<index->settings->attribute_size;i++){
                temp->attribute_when_searchedFirst[i]=attribute[i];
            }
            }else{
            temp->attribute_when_searchedFirst = malloc(sizeof(attribute_type)*index->settings->attribute_size);
            for(int i =0;i<index->settings->attribute_size;i++){
                temp->attribute_when_searchedFirst[i]=attribute[i];
            }
            }
            temp->nextInSkiplist =NULL;

            if(leafContainsAttr(temp,attribute,index)){
                temp->supports_pred =1;

                if(history_count>0){
                isax_node* temp2 = history[0];
                for(int i = 0 ; i<history_count;i++){
                    temp2= history[i];
                    temp2->nextInSkiplist = temp;
                }
                }
                free(history);
                return 1;
            }else{
                temp->supports_pred =2;
                history[history_count]=temp;
                history_count++;
            }


        }else if(temp->supports_pred ==1 || temp->supports_pred == 2 ){
            //printf("ATTRIBUTE:%d argument attribute: %d support:%d has %d L\n",*temp->attribute_when_searchedFirst,*attribute,temp->supports_pred,leafContainsAttr(temp,attribute));

            if(temp->supports_pred ==1){
                if(history_count>0){
                    isax_node* temp2 = history[0];
                    for(int i = 0 ; i<history_count;i++){
                        temp2= history[i];
                        temp2->nextInSkiplist = temp;
                    }
                }
                free(history);
                return 1;
            }else{
                history[history_count]=temp;
                history_count++;
            }

        }

        if(temp->nextInSkiplist!=NULL){

            if(temp->nextInSkiplist->leaf_id >max){
                if(history_count>0){
                    isax_node* temp2 = history[0];
                    for(int i = 0 ; i<history_count;i++){   
                        temp2= history[i];
                        if(temp2!=temp){
                            temp2->nextInSkiplist = temp;
                        }
                    }
                }
                free(history);
                return 0;
            }
            temp = temp->nextInSkiplist;

        }else{

            if(temp->leaflist_next == NULL || temp->leaflist_next->leaf_id > max){
                if(history_count>0){
                    isax_node* temp2 = history[0];
                    for(int i = 0 ; i<history_count;i++){   
                        temp2= history[i];
                        if(temp2!=temp){
                            temp2->nextInSkiplist = temp;
                        }
                    }
                }
                free(history);
                return 0;
            }
            temp=temp->leaflist_next;

        }

    }
    return 0;
    
}

struct StackNode {
    isax_node* data;
    int time;
    struct StackNode* next;
};

struct StackNode* newNode(isax_node* data,int time)
{
    struct StackNode* stackNode = 
      (struct StackNode*)
      malloc(sizeof(struct StackNode));
    stackNode->data = data;
    stackNode->time = time;
    stackNode->next = NULL;
    return stackNode;
}

int isEmpty(struct StackNode* root)
{
    return !root;
}

void push(struct StackNode** root, isax_node* data,int time)
{
    struct StackNode* stackNode = newNode(data,time);
    stackNode->next = *root;
    *root = stackNode;
    printf("%d pushed to stack\n", data);
}

struct StackNode* pop(struct StackNode** root)
{
    if (isEmpty(*root))
        return NULL;
    struct StackNode* temp = *root;
    *root = (*root)->next;
    printf("%d poped from stack\n", temp->data);
    return temp;
}

struct StackNode* peek(struct StackNode* root)
{
    if (isEmpty(root))
        return NULL;
    return root;
}

isax_node* filteredBsfTraversal(isax_node*node,attribute_type* attribute,isax_index *index,sax_type*sax){
    if(node==NULL)return NULL;
    if(node->is_leaf){
        if(evalSubtree(node,attribute,index)){
            return node;
        }else{
            return NULL;
        }
    }
        int location = index->settings->sax_bit_cardinality - 1 -
        node->split_data->split_mask[node->split_data->splitpoint];
        root_mask_type mask = index->settings->bit_masks[location];
        if(sax[node->split_data->splitpoint] & mask){
                isax_node * target = filteredBsfTraversal(node->right_child,attribute,index,sax);
                if(target==NULL){
                    return filteredBsfTraversal(node->left_child,attribute,index,sax);
                }else{
                    return target;
                }
        }else{
                isax_node * target = filteredBsfTraversal(node->left_child,attribute,index,sax);
                if(target==NULL){
                    return filteredBsfTraversal(node->right_child,attribute,index,sax);
                }else{
                    return target;
                }
        }
    

}
//////////////////////////////////////

//////////////////////////////////////////////////////////////////////
query_result  approximate_search_inmemory_pRecBuf_vorgias(ts_type *ts, ts_type *paa, isax_index *index,attribute_type * attribute)
{
    /////////////////////////////
    //int key = *attribute;
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
        
//        Traverse tree 1st way
        // while (!node->is_leaf) {
        //     int location = index->settings->sax_bit_cardinality - 1 -
        //     node->split_data->split_mask[node->split_data->splitpoint];
        //     root_mask_type mask = index->settings->bit_masks[location];

        //     if(sax[node->split_data->splitpoint] & mask)
        //     {
        //         //////////////////////////////////
        //         if(evalSubtree(node->right_child,attribute,index)){
        //         //////////////////////////////////
        //         node = node->right_child;
        //         ///////////////
        //         }else{
        //             //printf("wanted right went left\n");
        //             node = node->left_child;
        //         }
        //         ///////////////
        //     }
        //     else
        //     {
        //         //////////////////////////////////
        //         if(evalSubtree(node->left_child,attribute,index)){
        //         //////////////////////////////////
        //         node = node->left_child;
        //         ///////////////////
        //         }else{
        //             //printf("wanted left went right");
        //             node = node->right_child;
        //         }
        //         ///////////////////
        //     }
        // }
        //needs fixing started getting segmentetion after fixing evaltree
        //2ondway
    // struct StackNode* root = NULL;
    // struct StackNode* tempStacknode = NULL;
    // push(&root,node,1);
    // while(true){
    //     if(peek(root)==NULL)break;
    //     while (!node->is_leaf) {
    //         int location = index->settings->sax_bit_cardinality - 1 -
    //         node->split_data->split_mask[node->split_data->splitpoint];
    //         root_mask_type mask = index->settings->bit_masks[location];

    //         if(sax[node->split_data->splitpoint] & mask)
    //         {
    //             node = node->right_child;
    //             push(&root,node,1);
    //         }
    //         else
    //         {
    //             node = node->left_child;
    //             push(&root,node,1);
    //         }
    //     }
    //     tempStacknode=pop(&root);//pop the leaf

    //     if(evalSubtree(node,attribute,index))break;
        
    //     while(peek(root)!=NULL && peek(root)->time == 2){
    //         tempStacknode = pop(&root);
    //     }
    //     if(tempStacknode->data->parent == NULL )break;
    //     if(tempStacknode->data == (isax_node*)(((isax_node*)(tempStacknode->data->parent))->right_child)){
    //         node = (isax_node*)(((isax_node*)(node->parent))->left_child);
    //     }else{
    //         node = (isax_node*)(((isax_node*)(node->parent))->right_child);
    //     }
    //     if(peek(root)!=NULL && peek(root)->time == 1){
    //         pop(&root);
    //         push(&root,node->parent,2);
    //     }
    // }
    //3rd way
        node = filteredBsfTraversal(node,attribute,index,sax);
        result.distance = calculate_node_distance_inmemory_with_attribute(index, node, ts, FLT_MAX,attribute);           // caclulate initial BSF/////////////////////////
        result.node = node;
        // result.distance = FLT_MAX;//calculate_node_distance_inmemory_with_attribute(index, node, ts, FLT_MAX,attribute);           // caclulate initial BSF/////////////////////////
        // result.node = NULL;
    }
    else {
        printf("approximate_search_inmemory_pRecBuf_ekosmas: NO BSF has been computed! Bad Luck...\n");fflush(stdout);
        result.node = NULL;
        result.distance = FLT_MAX;
    }

    free(sax);

    return result;
}
/////////////////////////////////////////////////////////////////////////////////////////////

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
                if(evalSubtree(node->right_child,key,index)){
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
                if(evalSubtree(node->left_child,key,index)){
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
        result.distance = calculate_node_distance_inmemory_with_attribute(index, node, ts, FLT_MAX,key);           // caclulate initial BSF/////////////////////////
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
float calculate_node_distance_inmemory_with_attribute (isax_index *index, isax_node *node, ts_type *query, float bsf,attribute_type* attribute) 
{
    if(node == NULL)return bsf;
    // If node has buffered data
    if (node->buffer != NULL) 
    {   
        for (int i=0; i<node->buffer->partial_buffer_size; i++) {
            if(areAttributesEqual(node->buffer->partial_attribute_buffer[i],attribute,index->settings->attribute_size)){//TO BE COMPARED PROPERLY
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

/////////////////////////////////////////////////////////////////////////////////////////////
float calculate_node_distance2_inmemory_vorgias (isax_index *index, isax_node *node, ts_type *query, ts_type *paa, float bsf,attribute_type* attribute,isax_node_record ** record) 
{
    // If node has buffered data
    if (node->buffer != NULL) 
    {
        for (int i=0; i<node->buffer->partial_buffer_size; i++) {
            
            if(areAttributesEqual(node->buffer->partial_attribute_buffer[i],attribute,index->settings->attribute_size)){ //TO BE COMPARED PROPERLY
                //printf("attr %d found in node %d\n",*attribute,node->leaf_id);
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
                    (*record)->attr = node->buffer->partial_attribute_buffer[i];
                    (*record)->sax = node->buffer->partial_sax_buffer[i];
                }  
            }
            }
        }
    }
    
    
    
    return bsf;
}
//////////////////////////////////////////////////////////////////////////////////

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
