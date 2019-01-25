#include <stdio.h>
#include <unistd.h>
#include "supergraph.h"
#include <string.h>
// #include <time.h>

typedef struct argument argument;

struct argument {
	query_helper* helper;
	post* posts;
	int id;
};

argument* argu;
//Queue implementation is modified and used based on the week 7 tutorial solution
//queue implementation
typedef struct queue queue;

struct queue {
	size_t n_elements;
	size_t size;
	size_t cur_position;
	int* elements;
};

queue* create_queue(size_t size) {
	queue* q = malloc(sizeof(queue));
	q->cur_position =0;
	q->size=size;
	q->n_elements =0;
	q->elements = malloc(sizeof(int)*size);
	return q;
}

void enqueue(queue* q, int element) {
	if(q->n_elements>=q->size) {
		return;
	}
	q->elements[(q->cur_position+q->n_elements)%q->size]=element;
	q->n_elements++;

}

int dequeue(queue* q) {
	if(q->n_elements==0){
		return -1;
	}
	int element=q->elements[q->cur_position];
	q->cur_position=(q->cur_position+1) % q->size;
	q->n_elements--;
	return element;
}

void clean(queue* q) {
	free(q->elements);
	free(q);
}

//counting reposts
size_t find_reposts_count(post* posts,size_t post_idx){
	size_t count=0;
	if(posts[post_idx].n_reposted==0){
		count++;
		return count;
	}
	for(int i=0;i<posts[post_idx].n_reposted;i++){
	count=count+find_reposts_count(posts,posts[post_idx].reposted_idxs[i]);
	}
	count++;
	return count;
}

//recurrence function for find_all_repost query, which is called by thread_func
//return a result struct pointer
int current_p_reposts=0; //cursor
void find_reposts(post* posts,size_t post_idx,post** result_list){
	result_list[current_p_reposts]=posts+post_idx;
	current_p_reposts++;
	if(posts[post_idx].n_reposted==0){
		return;
	}
	for(int i=0;i<posts[post_idx].n_reposted;i++){
		find_reposts(posts,posts[post_idx].reposted_idxs[i],result_list);
	}
}

//recurrence function for find_original query, which is called by thread func
result* find_original_rec(post* posts,size_t count,uint64_t post_id){
	post** result_list;
	result* original=NULL;
	int i,j;
	uint64_t parent_id;
	int isoriginal=1;//true it is original
	//checking the post correspond to given post_id is original or not
	for(i=0;i<count;i++){
		for(j=0;j<posts[i].n_reposted;j++){
			if(posts[posts[i].reposted_idxs[j]].pst_id==post_id){
				isoriginal=0;
				parent_id=posts[i].pst_id;
			}
		}
	}
	if(isoriginal==1){//true the given post is original
		original=(result*)malloc(sizeof(result));
		result_list=(post**)malloc(sizeof(post*));
		int index=0; // index for given post 
		for(i=0;i<count;i++){
			if(post_id==posts[i].pst_id){
					index=i;
					break;
			}
		}
		result_list[0]=&posts[index];
		original->n_elements=1;
		original->elements=(void**)result_list;
	}else{ // given post is not an orginal, check its parent post is original or not
		find_original_rec(posts,count,parent_id);
	}
	return original;
}

void* thread_func(void* arg){
	argument* args=(argument*)arg;
	int id=args->id;
	while(1){
		pthread_mutex_lock(&args->helper->mutex);
		while(args->helper->cur_num_work==0&&args->helper->pool_close==0){//not close
			pthread_cond_wait(&args->helper->no_work,&args->helper->mutex);
		}
		if(args->helper->pool_close==1){
			pthread_mutex_unlock(&args->helper->mutex);
            pthread_exit(NULL);
			//break;//destroy the pool
		}
		pthread_mutex_unlock(&args->helper->mutex);
		
		if(args->helper->which_func==1){
			args->helper->num_elements[id-1]=0;
			for(int i=0;i<args->helper->length[id-1];i++){
			args->helper->num_elements[id-1]=args->helper->num_elements[id-1]+find_reposts_count(args->helper->post,args->helper->post_idx[id-1][i]);
			// printf("arg_num %lu\n",args->helper->num_elements[id-1]);
			}
		}else if(args->helper->which_func==2){
			
		}else if(args->helper->which_func==3){
			
		}else if(args->helper->which_func==4){
			
		}
		pthread_barrier_wait(&args->helper->barrier);
		args->helper->cur_num_work=0;
		pthread_mutex_lock(&args->helper->mutex);
		args->helper->thread_is_finish=1;//is finished
		pthread_cond_signal(&args->helper->full_work);
		pthread_mutex_unlock(&args->helper->mutex);
	}
	return NULL;
}


query_helper* engine_setup(size_t n_processors) {
argu=(argument*)malloc(sizeof(argument)*n_processors);
	query_helper* helper=(query_helper*)malloc(sizeof(query_helper));
	helper->num_threads=n_processors;
	helper->cur_num_work=0;
	helper->thread_is_finish=1; //is finished
	helper->cursor_for_split=0; //begin of array
	if(pthread_mutex_init(&(helper->mutex),NULL)){
		printf("failed to init mutex!\n");
		return NULL;
	}
	if(pthread_cond_init(&(helper->no_work),NULL)){
		printf("failed to init con no_work!\n");
		return NULL;
	}
	if(pthread_cond_init(&(helper->full_work),NULL)){
		printf("failed to init con full_work!\n");
		return NULL;
	}
	if(pthread_barrier_init(&(helper->barrier),NULL,n_processors)){
		printf("failed to init con full_work!\n");
		return NULL;
	}
	helper->pool_close=0;// not close
	helper->threads=(pthread_t*)malloc(sizeof(pthread_t)*n_processors);
	helper->result_block=(result*)malloc(sizeof(result));
	helper->result_block->n_elements=0;
	for(int i=0;i<n_processors;i++){
		argu[i].helper=helper;
		argu[i].id=i+1;
		pthread_create(helper->threads+i,NULL,thread_func,(void*)(argu+i));
	}
	return helper;
	//return NULL;
}


result* find_all_reposts(post* posts, size_t count, uint64_t post_id, query_helper* helper) {
	// clock_t begin = clock();
	result* result_block;
	int i;
	int count_element=0;
	int original_idx;
	for(i=0;i<count;i++){ 
		if(post_id==posts[i].pst_id){
		original_idx=i;	
		count_element=count_element+1;
		break;
		}
	}
	if(count_element==0){  //given post_id does not exist
		helper->pool_close=1;
		pthread_cond_broadcast(&helper->no_work);
		result_block=(result*)malloc(sizeof(result)*1);
		result_block->elements=NULL;
		result_block->n_elements=0;
		return result_block;
	}else{
	pthread_mutex_lock(&helper->mutex);
		helper->post_idx=(size_t**)malloc(sizeof(size_t*)*helper->num_threads);
		int length[helper->num_threads];
		//int sub=(int)posts[original_idx].n_reposted/(int)helper->num_threads;
		int remain=(int)posts[original_idx].n_reposted%(int)helper->num_threads;
		int cursor=0;
		for(i=0;i<helper->num_threads;i++){
			int sub=(int)posts[original_idx].n_reposted/(int)helper->num_threads;
			if(remain>0){
				sub++;
			}
			helper->post_idx[i]=(size_t*)malloc(sizeof(size_t)*sub);
			for(int j=0;j<sub;j++){
				helper->post_idx[i][j]=posts[original_idx].reposted_idxs[cursor+j];
			}
			length[i]=sub;
			remain--;
			cursor=cursor+sub;
		}
		helper->num_elements=(size_t*)malloc(sizeof(size_t)*helper->num_threads);
		helper->length=length;
		helper->count=count;
		helper->post=posts;
		helper->cur_num_work=1;
		helper->thread_is_finish=0;//not finished
		helper->which_func=1;
		pthread_cond_broadcast(&helper->no_work);
		while(helper->thread_is_finish==0){ //not finished, wait for thread
			pthread_cond_wait(&helper->full_work,&helper->mutex);
		}
		pthread_mutex_unlock(&helper->mutex);
		for(i=0;i<helper->num_threads;i++){
			helper->result_block->n_elements=helper->result_block->n_elements+helper->num_elements[i];
		}
		helper->result_block->n_elements++; //add original
		helper->pool_close=1;
		pthread_cond_broadcast(&helper->no_work);
	//using recurrence, whithout using thread to split array, it is not an opt solution
		// size_t number_of_repost=find_reposts_count(posts,original_idx);
		helper->result_list=(post**)malloc(sizeof(post*)*helper->result_block->n_elements);
		find_reposts(posts,original_idx,helper->result_list);
		helper->result_block->elements=(void**)helper->result_list;
	}
	// clock_t end = clock();
	// double time_elapsed = (double)(end - begin) / CLOCKS_PER_SEC;
	// printf("Time Elapsed: %f\n", time_elapsed);
	return helper->result_block;
}

//currently it is a recurrence solution in sequential way, which will be imprve by multi thread later
result* find_original(post* posts, size_t count, uint64_t post_id, query_helper* helper) {
	helper->pool_close=1;
	pthread_cond_broadcast(&helper->no_work);
	
	result* result_block;
	int i,j;
	post** result_list;
	int count_element=0;
	for(i=0;i<count;i++){
		if(post_id==posts[i].pst_id){
		count_element=count_element+1;
		}
	}
	if(count_element==0){ // given post_id is not exist
		result_block=(result*)malloc(sizeof(result)*1);
		result_block->elements=NULL;
		result_block->n_elements=0;
		return result_block;
	}else{
		uint64_t parent_id;
		int isoriginal=1;//false;
		for(i=0;i<count;i++){
			for(j=0;j<posts[i].n_reposted;j++){
				if(posts[posts[i].reposted_idxs[j]].pst_id==post_id){
					isoriginal=0;
					parent_id=posts[i].pst_id;
				}
			}
		}
		if(isoriginal==1){
			result_block=(result*)malloc(sizeof(result));
			result_list=(post**)malloc(sizeof(post*));
			int index=0;
			for(i=0;i<count;i++){
				if(post_id==posts[i].pst_id){
					index=i;
					break;
				}
			}
			result_list[0]=posts+index;
			result_block->elements=(void**)result_list;
			result_block->n_elements=1;
			return result_block;
		}else{
			return find_original_rec(posts,count,parent_id);
		}
	}
	return NULL;
}


size_t build_shortest_path_counting(int* parent_of_node,int cur_index,int userA_idx,int count,result* result_block){
		count++;
		if(userA_idx==cur_index){
			result_block->n_elements=count;
			return result_block->n_elements;
		}
		else{
			result_block->n_elements=count;
			if(parent_of_node[cur_index]==-1){
				return 0;
			}
			build_shortest_path_counting(parent_of_node,parent_of_node[cur_index],userA_idx,count,result_block);
		}
	return result_block->n_elements;
}

int current_p_path=0;
void build_shortest_path(int* parent_of_node,int cur_index,int userA_idx, user** result_list,user* users,result* result_block){
		if(userA_idx==cur_index){
			result_list[current_p_path]=users+cur_index;
			result_block->elements=(void**)result_list;
			return;
		}
		else{
			result_list[current_p_path]=users+cur_index;
			current_p_path++;
			result_block->elements=(void**)result_list;
			build_shortest_path(parent_of_node,parent_of_node[cur_index],userA_idx,result_list,users,result_block);
		}
	current_p_path++;
	return;
}


result* shortest_user_link(user* users, size_t count, uint64_t userA, uint64_t userB, query_helper* helper) {
	helper->pool_close=1;
	pthread_cond_broadcast(&helper->no_work);
	
	if(userA==userB){
		return NULL;
	}
	//boolean array  0 for not   1 for yes
	int isvisited[count];
	//parent array;
	int parent_of_node[count];
	
	int counting=0;
	
	int i;
	int user_idx_A;
	int user_idx_B;
	for(i=0;i<count;i++){
		isvisited[i]=0;
		parent_of_node[i]=-1;
		if(users[i].user_id==userA){
			counting++;
			user_idx_A=i;
		}
		if(users[i].user_id==userB){
			counting++;
			user_idx_B=i;
		}
	}
	if(counting<2){
		return NULL;
	}
	
	//build queue
	queue* q=create_queue(count);
	
	enqueue(q,user_idx_A);
	int user_idx;
	int following_idx;
	
	while(q->n_elements!=0){
		user_idx=dequeue(q);
		for(i=0;i<users[user_idx].n_following;i++){
			following_idx=users[user_idx].following_idxs[i];
			if(isvisited[following_idx]==0){
				isvisited[following_idx]=1;
				parent_of_node[following_idx]=user_idx;
				enqueue(q,following_idx);
			}
		}
	}
	
	result* result_block=(result*)malloc(sizeof(result));
	size_t a_b_size=build_shortest_path_counting(parent_of_node,user_idx_B,user_idx_A,0,result_block);
	user** result_list=(user**)malloc(sizeof(user*)*a_b_size);
	if(a_b_size>0){
 	build_shortest_path(parent_of_node,user_idx_B,user_idx_A,result_list,users,result_block);
	}
	
	current_p_path=0;
	//havepath=0;
	queue* q2=create_queue(count);
	
	//boolean array  0 for not   1 for yes
	int isvisited_2[count];
	//parent array;
	int parent_of_node_2[count];
	
	for(i=0;i<count;i++){
		isvisited_2[i]=0;
		parent_of_node_2[i]=-1;
	}

	enqueue(q2,user_idx_B);
	while(q2->n_elements!=0){
		user_idx=dequeue(q2);
		for(i=0;i<users[user_idx].n_following;i++){
			following_idx=users[user_idx].following_idxs[i];
			if(isvisited_2[following_idx]==0){
				isvisited_2[following_idx]=1;
				parent_of_node_2[following_idx]=user_idx;
				enqueue(q2,following_idx);
			}
		}
	}
	
	result* result_block_2=(result*)malloc(sizeof(result));
	size_t b_a_size=build_shortest_path_counting(parent_of_node_2,user_idx_A,user_idx_B,0,result_block_2);
	user** result_list_2=(user**)malloc(sizeof(user*)*b_a_size);
	if(b_a_size>0){
	build_shortest_path(parent_of_node_2,user_idx_A,user_idx_B,result_list_2,users,result_block_2);
	}
	result* result_final=(result*)malloc(sizeof(result));
	
	user** result_list_final_1=(user**)malloc(sizeof(user*)*a_b_size);
	user** result_list_final_2=(user**)malloc(sizeof(user*)*b_a_size);
	int index=0;
	if(a_b_size>0){
		for(i=a_b_size-1;i>=0;i--){
			result_list_final_1[index]=result_block->elements[i];
			index++;
		}
	}
	index=0;
	if(b_a_size>0){
		for(i=b_a_size-1;i>=0;i--){
			result_list_final_2[index]=result_block_2->elements[i];
			index++;
		}
	}
	if(a_b_size<b_a_size&&a_b_size!=0){
		result_final->elements=(void**)result_list_final_1;
		result_final->n_elements=a_b_size;
		free(result_list_final_2);
	}else if(a_b_size>b_a_size&&b_a_size!=0){
		result_final->elements=(void**)result_list_final_2;
		result_final->n_elements=b_a_size;
		free(result_list_final_1);
	}else if(a_b_size==0){
		result_final->elements=(void**)result_list_final_2;
		result_final->n_elements=b_a_size;
		free(result_list_final_1);
	}else if(b_a_size==0){
		result_final->elements=(void**)result_list_final_1;
		result_final->n_elements=a_b_size;
		free(result_list_final_2);
	}
	
	clean(q);
	clean(q2);
	free(result_list);
	free(result_list_2);
	free(result_block);
	free(result_block_2);
	
	
	return result_final;
}

result* find_bots(user* users, size_t user_count, post* posts, size_t post_count, criteria* crit, query_helper* helper) {
	helper->pool_close=1;
	pthread_cond_broadcast(&helper->no_work);
	
	if(crit->oc_threshold<0||crit->oc_threshold>1||crit->acc_rep_threshold<0||crit->acc_rep_threshold>1||crit->bot_net_threshold<0||crit->bot_net_threshold>1){
		result* result_block=(result*)malloc(sizeof(result)*1);
		result_block->elements=NULL;
		result_block->n_elements=0;
		return result_block;
	}
	
	size_t user_repost_num=0;
	float oc_threshold;
	float acc_rep_threshold;
	float bot_net_threshold;
	result* result_block=(result*)malloc(sizeof(result));
	int* robot_indx=(int*)malloc(sizeof(int));
	size_t robt_count=0;
	int i,j,k;
	size_t* reposted_pool=(size_t*)malloc(sizeof(size_t));
	size_t reposted_count=0;
	int isrobot[user_count];
	
	for(i=0;i<user_count;i++){
		isrobot[i]=0;
		for(j=0;j<users[i].n_posts;j++){
			for(k=0;k<posts[users[i].post_idxs[j]].n_reposted;k++){
			reposted_count++;
			reposted_pool=realloc(reposted_pool,reposted_count*sizeof(size_t));
			reposted_pool[reposted_count-1]=posts[users[i].post_idxs[j]].reposted_idxs[k];
			}
		}
	}
	for(i=0;i<user_count;i++){
		if(isrobot[i]==1){
			continue;
		}
		for(j=0;j<users[i].n_posts;j++){
			for(k=0;k<reposted_count;k++){
				if(reposted_pool[k]==users[i].post_idxs[j]){
					user_repost_num++;
				}
			}
		}
		oc_threshold=(float)user_repost_num/(float)users[i].n_posts;
		int sum_follow=users[i].n_followers+users[i].n_following;
		acc_rep_threshold=(float)users[i].n_followers/(float)sum_follow;
		if(oc_threshold>crit->oc_threshold){
			robt_count++;
			robot_indx=realloc(robot_indx,sizeof(int)*robt_count);
			robot_indx[robt_count-1]=i;
			isrobot[i]=1;//is robot;
		}
		else if(acc_rep_threshold<crit->acc_rep_threshold){
			robt_count++;
			robot_indx=realloc(robot_indx,sizeof(int)*robt_count);
			robot_indx[robt_count-1]=i;
			isrobot[i]=1;//is robot;
		}
		user_repost_num=0;
	}
	//discrete
	int user_robt_following;
	for(i=0;i<user_count;i++){
		for(j=0;j<users[i].n_followers;j++){
			if(isrobot[users[i].follower_idxs[j]]==1){
				for(k=0;k<robt_count;k++){
					if(users[i].follower_idxs[j]==robot_indx[k]){
						user_robt_following=user_robt_following+1;
					}
				}
			}
		}
		bot_net_threshold=(float)user_robt_following/(float)users[i].n_followers;
		if(bot_net_threshold>crit->bot_net_threshold&&isrobot[i]==0){
			robt_count++;
			robot_indx=realloc(robot_indx,sizeof(int)*robt_count);
			robot_indx[robt_count-1]=i;
			isrobot[i]=1;//is robot;
			user_robt_following=0;
			i=-1;
		}
		user_robt_following=0;
	}
		user** robot_list=(user**)malloc(sizeof(user*)*robt_count);
		for(i=0;i<robt_count;i++){
			robot_list[i]=&users[robot_indx[i]];
		}
		result_block->elements=(void**)robot_list;
		result_block->n_elements=robt_count;
	free(robot_indx);
	free(reposted_pool);
	return result_block;
}

void engine_cleanup(query_helper* helpers) {
for(int i=0;i<helpers->num_threads;i++){
		pthread_join(helpers->threads[i],NULL);
	}
	pthread_mutex_destroy(&helpers->mutex);
	pthread_cond_destroy(&helpers->full_work);
	pthread_cond_destroy(&helpers->no_work);
	pthread_barrier_destroy(&helpers->barrier);
	free(helpers->threads);
	if(helpers->which_func!=1){
	free(helpers->result_block);
	}
	if(helpers->which_func==1){
	for(int i=0;i<helpers->num_threads;i++){
		free(helpers->post_idx[i]);
	}
	free(helpers->post_idx);
	free(helpers->num_elements);
	}
	free(helpers);
	free(argu);
}
