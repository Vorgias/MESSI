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
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_only_after_helper(void *rfdata)
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
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_no_helping(void *rfdata)
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
void* exact_search_worker_inmemory_hybridpqueue_ekosmas_lf_sorted_array(void *rfdata)
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

    // // A.2. Create my priority queue (Heap)
    // if (!input_data->allpq[0] && input_data->next_queue_data_pos[0]) {
    //     create_heap_from_data_queue(0, input_data->allpq_data, input_data->next_queue_data_pos, input_data->allpq);
    // }

    // A.2. Create my priority queue (Heap)
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