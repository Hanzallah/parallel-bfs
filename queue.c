#include <stdio.h>
#include <stdlib.h>
#include "util.h"

struct frontier *createFrontier()
{
    struct frontier *f = malloc(sizeof(struct frontier));
    f->front = -1;
    f->rear = -1;
    return f;
}

void enqueue(struct frontier *f, int value)
{
    if (f->rear == SIZE - 1)
    {
        printf("Frontier is full!");
    }
    else
    {
        if (f->front == -1)
        {
            f->front = 0;
        }
        f->rear++;
        f->values[f->rear] = value;
    }
}

int dequeue(struct frontier *f)
{
    int value;
    if (isEmpty(f))
    {
        printf("Frontier is empty!");
        value = -1;
    }
    else
    {
        value = f->values[f->front];
        f->front++;
        if (f->front > f->rear)
        {
            f->front = f->rear = -1;
        }
    }
    return value;
}

int isEmpty(struct frontier *f)
{
    if (f->rear == -1)
    {
        return 1;
    }
    return 0;
}

void printQueue(struct frontier *f)
{
    if (!isEmpty(f))
    {
        printf("[ ");
        for (int i = f->front; i < f->rear + 1; i++)
        {
            printf("%d ", f->values[i]);
        }
        printf("]\n");
    }
    else{
        printf("[]\n");
    }
}
