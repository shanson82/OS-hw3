#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

#define 	N_THREADS 	30
#define		CHUNK_SIZE	300


typedef struct word_bag {
	char 	*word;
	int		freq;
	struct word_bag *next;
} word_bag_t;

typedef struct {
	char buf[CHUNK_SIZE + 1];	// add one more byte to hold 
								// the zero for zero-terminated string
	word_bag_t *head;			// head of the list
	int tid;					// thread id
} thread_info_t;

thread_info_t thread_info[N_THREADS];	// per thread information
pthread_t threads[N_THREADS];			// pthread info
int sentence = 0;						// count the sentence (i.e., chunk) number
										// valid only for single threaded case

// look for a word in the list
// return null if it is not found or a pointer to it
word_bag_t *find_word(word_bag_t *head, char *word) {
	if (head == NULL) {
		return NULL;
	} else {
		word_bag_t *t;
		for (t = head; t != NULL; t = t->next) {
			if (strcmp(t->word, word) == 0) {
				return t;
			}
		}
		return NULL;
	}
}

// create a new word and add it as the new head of the list
// note: double pointer is required to allow the value of the head to be updated
word_bag_t *new_word(word_bag_t **head, char *word) {
	word_bag_t *bag = (word_bag_t *) malloc(sizeof(word_bag_t));
	bag->next = NULL;
	bag->word = word;
	bag->freq = 0;
	
	bag->next = *head;
	*head = bag;	
	return bag;
}

// count the word that we found
// the word is between buf[start] to buf[end]
// note: start == end when this is a single char word
void count_word(word_bag_t **head, char *buf, int start, int end) {
	assert(end >= start);
	char *new_word_str = (char *) malloc(end - start + 2);
	strncpy(new_word_str, &buf[start], end - start + 1);
	new_word_str[end - start + 1] = 0;  
	
	// add it to linked list if it does not exist
	// or find where it is in the linked list;
	word_bag_t *word = find_word(*head, new_word_str);
	if (word == NULL) {
		printf("\tnew word [%s]\n", new_word_str);
		word = new_word(head, new_word_str);
	} else {
		free(new_word_str);
	}
	word->freq = word->freq + 1;
	
	printf("\tfound_word [%s] %d\n", word->word, word->freq);
}

// splits the content into words
void *count_words(void *argin) {
	thread_info_t *arg = (thread_info_t *) argin;	
	word_bag_t **head = &arg->head;
	
	char *buf = arg->buf;
	printf("sentence [%s] %d\n", buf, sentence);
	sentence++;
	
	//starts with leading spaces, skip them
	int i;
	for (i = 0; i <= strlen(buf); i++) {
		if (buf[i] != ' ') break;
	}
	
	int start = i, end = i; 
	for (; i <= strlen(buf); i++) {
		if (buf[i] == ' ') {
			if (i > 0) end = i - 1;
			count_word(head, buf, start, end);
			
			// we need to eat the remaining white spaces			
			for (;i <= strlen(buf); i++) {
				if (buf[i] != ' ') break;
			}
			start = i;
		} 			
	}

	if (start != strlen(buf)) {
		count_word(head, buf, start, strlen(buf));
	}

	return NULL;
}


// merge the lists of threads
void merge() {
	word_bag_t *head = NULL;
	int tid;
	for (tid = 0; tid < N_THREADS; tid++) {
		// iterate to each thread's list
		word_bag_t *t;
		for (t = thread_info[tid].head; t != NULL; t = t->next) {
			word_bag_t *word_ptr = find_word(head, t->word);
			if (word_ptr == NULL) {
				word_ptr = new_word(&head, t->word);
			} 
			word_ptr->freq = word_ptr->freq + t->freq;
		}
	}
	
	printf("\n\n\n");
	printf("******** FINAL WORD COUNT ********\n");
	word_bag_t *t;
	for (t = head; t != NULL; t = t->next) {
		 printf("final [%s] %d\n", t->word, t->freq);
	}
	
}

int main(int argc, char **argv) {
	FILE *f = fopen(argv[1], "r");
	if (f == NULL) {
		printf("File not found\n");
		return -1;
	}
	
	int tid;
	for (tid = 0; tid < N_THREADS; tid++) {
		thread_info[tid].head = NULL;
	}
	
	while(!feof(f)) {
		for (tid = 0; tid < N_THREADS; tid++) {
			int sz = fread(thread_info[tid].buf, 1, CHUNK_SIZE, f);	
			thread_info[tid].buf[sz] = 0;
			
			printf("[%s] %d\n", thread_info[tid].buf, sz);
			int end = sz; 
			if (!feof(f)) {
				for (end = sz; end >= 0; end--) {
					if ((thread_info[tid].buf[end] == ' ') || (thread_info[tid].buf[end] == '\t')) break;				
				}			
			}
			// you are on a white space at thread_info[i].buf[end]			
			thread_info[tid].buf[end] = 0; //ensure that it is null-terminated		
			printf("[%s] %d %d\n", thread_info[tid].buf, sz, end);
			
			
			// ==> needs to be given to each new thread
			thread_info[tid].tid = tid;
			int status = pthread_create(&threads[tid], NULL, count_words, (void *)&thread_info[tid]);
		
			if(status !=0){
				printf("pthread error code %d\n", status);
				exit(-1);
			} else {
				printf("Created thread %d\n", tid);
			}
		
			if (feof(f)) break;
			
			// go backward to account for the bytes we removed
			fseek(f, end - sz + 1, SEEK_CUR);			
		}
		
		for (tid = 0; tid < N_THREADS; tid++) {	
			pthread_join(threads[tid], NULL);	
		}
	}
	fclose(f);

	
	// merge the lists
	merge();
	
	return 0;
};
