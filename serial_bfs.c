#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "util.h"

int main(int argc, char *argv[])
{
    if (argc != 4)
    {
        printf("Wrong argments usage: serial_bfs [DATA_FILE] [sourceVertex] [OUTPUT_FILE]\n");
    }

    int sourceVertex = atoi(argv[2]);

    int numVertices, numEdges, *offsets, *edges;
    int success = read_file(argv[1], &numVertices, &numEdges, &offsets, &edges);

    if (success)
    {
        // The BFS algorithm
        struct frontier* cur_frontier = createFrontier();
        struct frontier* next_frontier = createFrontier();
        enqueue(cur_frontier, sourceVertex);

        int distance[numVertices];
        for (int  i = 0; i < numVertices; i++){
            distance[i] = INT_MAX;
        }
        distance[sourceVertex] = 0;

        int level = 1;

        while (!isEmpty(cur_frontier)){
            for (int i = cur_frontier->front; i <= cur_frontier->rear; i++){
                for (int j = offsets[cur_frontier->values[i]]; j < offsets[cur_frontier->values[i] + 1] ; j++){
                    if (distance[edges[j]] == INT_MAX){
                        distance[edges[j]] = level;
                        enqueue(next_frontier, edges[j]);
                    }
                }
            }
            cur_frontier = next_frontier;
            next_frontier = createFrontier();
            level++;
        }

        // save to output file
        FILE *to = fopen(argv[3], "w");
        for (int  i = 0; i < numVertices; i++){
            if (distance[i] == INT_MAX){
                distance[i] = -1;
            }
            fprintf(to,"%d\n", distance[i]);
        }
    }

    return 0;
}