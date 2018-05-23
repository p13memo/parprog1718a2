// simple quicksort implementation
// compile with: gcc -O2 -Wall quicksort.c -o quicksort

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <stdbool.h>

#include <pthread.h>

#define CIRCULAR_QUEUE_SIZE 1000
#define N 100000
#define CUTOFF 100


typedef struct quicksort_data {
	double *a;
	unsigned pivot;
	unsigned start;
	unsigned end;
} quicksort_data;

typedef struct circular_queue {
   int front,rear;
   unsigned capacity;
   quicksort_data *array;
} circular_queue;

typedef struct thread_data {
	bool shutdown;
	int sorted_elements;
} thread_data;

bool circular_queue_full(circular_queue* q) {
    return ( (q->front == q->rear + 1) || (q->front == 0 && q->rear == CIRCULAR_QUEUE_SIZE-1));
}

bool circular_queue_empty(circular_queue* q) {
    return (q->front == -1);
}

void enqueue(circular_queue* q, quicksort_data element) {
    if(circular_queue_full(q)) return;
    else {
        if(q->front == -1) q->front = 0;
        q->rear = (q->rear + 1) % CIRCULAR_QUEUE_SIZE;
        q->array[q->rear] = element;
    }
}

quicksort_data dequeue(circular_queue* q) {
    quicksort_data element;
    if(circular_queue_empty(q)) {
        return element;
    }
    else {
        element = q->array[q->front];
        if (q->front == q->rear) {
        	q->front = -1;
        	q->rear = -1;
        }
        else {
        	q->front = (q->front + 1) % CIRCULAR_QUEUE_SIZE;
        }
        return element;
    }
}

// Circular queue
circular_queue global_buffer;

// condition variable, signals a put operation (receiver waits on this)
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
// condition variable, signals a get operation (sender waits on this)
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;
// condition variable, signals an update on sorted element count
pthread_cond_t sorted = PTHREAD_COND_INITIALIZER;
// mutex protecting common resources
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void inssort(double *a, unsigned n) {
	unsigned i, j;
	double t;

	for (i = 1; i < n; i++) {
		j = i;
		while ((j > 0) && (a[j - 1] > a[j])) {
			t = a[j - 1];
			a[j - 1] = a[j];
			a[j] = t;
			j--;
		}
	}
}

quicksort_data get_job_from_circular_queue(thread_data* thr_data) {
	pthread_mutex_lock(&mutex);
	while (circular_queue_empty(&global_buffer)) {// Queue is empty, wait for the producer to add values
		pthread_cond_wait(&msg_in, &mutex);
		if (thr_data->shutdown) {
			pthread_exit(NULL);
		}
	}
	quicksort_data q = dequeue(&global_buffer);
	pthread_cond_broadcast(&msg_out);
	pthread_mutex_unlock(&mutex);
	return q;
}

void perform_insertion_sort(double* a, int n, thread_data* thr_data) {
	if (n > 1) inssort(a, n);
	pthread_mutex_lock(&mutex);
	thr_data->sorted_elements += n;
	pthread_cond_broadcast(&sorted);
	pthread_mutex_unlock(&mutex);
}

int perform_quicksort(double* a, unsigned n) {
	int first, last, middle;
	double t, p;
	int i, j;

	// take first, last and middle positions
	first = 0;
	middle = n - 1;
	last = n / 2;

	// put median-of-3 in the middle
	if (a[middle] < a[first]) {
		t = a[middle];
		a[middle] = a[first];
		a[first] = t;
	}
	if (a[last] < a[middle]) {
		t = a[last];
		a[last] = a[middle];
		a[middle] = t;
	}
	if (a[middle] < a[first]) {
		t = a[middle];
		a[middle] = a[first];
		a[first] = t;
	}

	// partition (first and last are already in correct half)
	p = a[middle]; // pivot
	for (i = 0, j = n - 1;; i++, j--) {
		while (a[i] < p)
			i++;
		while (p < a[j])
			j--;
		if (i >= j)
			break;

		t = a[i];
		a[i] = a[j];
		a[j] = t;
	}
	return i;
}

void add_jobs_to_circular_queue(double* a, int new_pivot, int n) {
	// send message
	quicksort_data thr1;
	thr1.a = a;
	thr1.pivot = new_pivot;
	quicksort_data thr2;
	thr2.a = a + new_pivot;
	thr2.pivot = n - new_pivot;
	pthread_mutex_lock(&mutex);
	while (circular_queue_full(&global_buffer)) { // If the queue is full
		pthread_cond_wait(&msg_out, &mutex); // wait until a msg is received - NOTE: mutex MUST be locked here.
	}
	enqueue(&global_buffer, thr1);
	pthread_cond_broadcast(&msg_in);
	enqueue(&global_buffer, thr2);
	pthread_cond_broadcast(&msg_in);
	pthread_mutex_unlock(&mutex);
}

// producer thread function
void *quicksort_thread_function(void *args) {

	thread_data *thr_data = (thread_data *)args;
	while (1) {
		quicksort_data q = get_job_from_circular_queue(thr_data);

		double* a = q.a;
		unsigned n = q.pivot;

		// check if below cutoff limit
		if (n <= CUTOFF) {
			perform_insertion_sort(a, n, thr_data);
		}
		else {
			int i = perform_quicksort(a, n);
			add_jobs_to_circular_queue(a, i, n);
		}
	}
	pthread_exit(NULL);
}


int main() {
	double *a;

	a = (double *) malloc(N * sizeof(double));
	if (a == NULL) {
		printf("error in malloc\n");
		exit(1);
	}

	// fill array with random numbers
	srand(time(NULL));
	for (unsigned i = 0; i < N; i++) {
		a[i] = (double) rand() / RAND_MAX;
	}

	global_buffer.front = -1;
	global_buffer.rear = -1;
	global_buffer.capacity = CIRCULAR_QUEUE_SIZE;
	global_buffer.array = (quicksort_data*)malloc(CIRCULAR_QUEUE_SIZE * sizeof(quicksort_data));

	quicksort_data data;
	data.a = a;
	data.pivot = N;
	enqueue(&global_buffer, data);

	thread_data thr_data;
	thr_data.shutdown = false;
	thr_data.sorted_elements = 0;
	pthread_t thread_pool[4];
	for (unsigned i=0; i<4; ++i) {
		pthread_create(&thread_pool[i], NULL, quicksort_thread_function, &thr_data);
	}

	pthread_mutex_lock(&mutex);
	while (thr_data.sorted_elements < N) {
		pthread_cond_wait(&sorted, &mutex);
	}
	thr_data.shutdown = true;
	pthread_mutex_unlock(&mutex);

	pthread_mutex_lock(&mutex);
	for (unsigned i=0; i<4; ++i)
		pthread_cond_broadcast(&msg_in);
	pthread_mutex_unlock(&mutex);

	for (unsigned i=0; i<4; ++i)
		pthread_cancel(thread_pool[i]);

	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&msg_out);
	pthread_cond_destroy(&msg_in);
	pthread_cond_destroy(&sorted);

	// check sorting
	for (unsigned i = 0; i < (N - 1); i++) {
		if (a[i] > a[i + 1]) {
			printf("Sort failed!\n");
			return -1;
		}
	}

	printf("Sorting succeeded\n");

	free(global_buffer.array);
	free(a);

	return 0;
}
