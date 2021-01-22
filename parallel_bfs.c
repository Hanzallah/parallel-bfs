#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <math.h>
#include "mpi.h"
#include "util.h"

int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int worldSize, rank, sourceVertex, partitionShow, numVertices, numEdges, *offsets, *edges;
    MPI_Status s;
    MPI_Request *r;

    // Get number of processes and check that 3 processes are used
    MPI_Comm_size(MPI_COMM_WORLD, &worldSize);

    // Get my rank
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (argc != 5)
    {
        printf("Wrong argments usage: mpirun -np X parallel_bfs [DATA_FILE] [sourceVertex] [partitionShow] [OUTPUT_FILE]\n");
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    sourceVertex = atoi(argv[2]);
    partitionShow = atoi(argv[3]);

    // Reading file and 1-D partitioning
    if (rank == 0)
    {
        // read the file
        int success = read_file(argv[1], &numVertices, &numEdges, &offsets, &edges);

        // if file reading is not successfull
        if (!success)
        {
            printf("File could not be read!");
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }

        // divide data for each processor
        for (int i = 1; i < worldSize; i++)
        {
            int procArrayStart = floor((numVertices * i) / worldSize);
            int procArrayLen = floor((numVertices * (i + 1)) / worldSize) - procArrayStart;

            int edgeLen = offsets[procArrayStart + procArrayLen] - offsets[procArrayStart];
            int locOffsets[procArrayLen];
            int locEdges[edgeLen];

            int j = 0;
            int k = 0;
            for (int i = procArrayStart; i < procArrayStart + procArrayLen; i++)
            {
                locOffsets[j] = offsets[i];
                for (int j = offsets[i]; j < offsets[i + 1]; j++)
                {
                    locEdges[k] = edges[j];
                    k++;
                }
                j++;
            }

            // send partitioned data to each processor
            MPI_Send((void *)&procArrayLen, 1, MPI_INT, i, 0xAC0F, MPI_COMM_WORLD);
            MPI_Send((void *)&numVertices, 1, MPI_INT, i, 0xAC4F, MPI_COMM_WORLD);
            MPI_Send((void *)&edgeLen, 1, MPI_INT, i, 0xAC1F, MPI_COMM_WORLD);
            MPI_Send((void *)&locOffsets, procArrayLen, MPI_INT, i, 0xAC2F, MPI_COMM_WORLD);
            MPI_Send((void *)&locEdges, edgeLen, MPI_INT, i, 0xAC3F, MPI_COMM_WORLD);
        }
    }

    int locNumEdges, locNumVertices, procArrayStart;
    if (rank != 0)
    {
        // receive partitioned data
        MPI_Recv(&numVertices, 1, MPI_INT, 0, 0xAC4F, MPI_COMM_WORLD, &s);
        MPI_Recv(&locNumVertices, 1, MPI_INT, 0, 0xAC0F, MPI_COMM_WORLD, &s);
        MPI_Recv(&locNumEdges, 1, MPI_INT, 0, 0xAC1F, MPI_COMM_WORLD, &s);
    }
    else
    {
        // generate partitions for root
        int procArrayStart = floor((numVertices * 0) / worldSize);
        locNumVertices = floor((numVertices * (0 + 1)) / worldSize) - procArrayStart;

        locNumEdges = offsets[procArrayStart + locNumVertices] - offsets[procArrayStart];
    }

    int locOffsets[locNumVertices + 1];
    locOffsets[locNumVertices] = locNumEdges;
    int locEdges[locNumEdges];

    if (rank != 0)
    {
        MPI_Recv(&locOffsets, locNumVertices, MPI_INT, 0, 0xAC2F, MPI_COMM_WORLD, &s);
        MPI_Recv(&locEdges, locNumEdges, MPI_INT, 0, 0xAC3F, MPI_COMM_WORLD, &s);
    }
    else
    {
        int j = 0;
        int k = 0;
        for (int i = procArrayStart; i < procArrayStart + locNumVertices; i++)
        {
            locOffsets[j] = offsets[i];
            for (int j = offsets[i]; j < offsets[i + 1]; j++)
            {
                locEdges[k] = edges[j];
                k++;
            }
            j++;
        }
    }


    // normalize offset array
    int normalizedOffset[locNumVertices + 1];
    for (int i = 0; i < locNumVertices; i++)
    {
        normalizedOffset[i] = locOffsets[i] - locOffsets[0];
    }
    normalizedOffset[locNumVertices] = locNumEdges;

    // print partitioned data if requested
    if (partitionShow)
    {
        printf("P%d: local_number_of_vertices=%d\n", rank, locNumVertices);
        int curr = locOffsets[0];
        int norm = 0;
        for (int i = 0; i < locNumVertices; i++)
        {
            if (curr != locOffsets[i])
                ++norm;

            printf("P%d: vertices[%d]=%d, normalized_vertices[%d]=%d\n", rank, i, locOffsets[i], i, norm);
        }

        printf("P%d: local_number_of_edges=%d\n", rank, locNumEdges);

        for (int i = 0; i < locNumVertices; i++)
        {
            for (int j = normalizedOffset[i]; j < normalizedOffset[i + 1]; j++)
            {
                printf("P%d: edges[%d]=%d\n", rank, i, locEdges[j]);
            }
        }
    }

    // The Parallel BFS Algorithm
    // distance array d
    int d[numVertices];
    for (int i = 0; i < numVertices; i++)
    {
        d[i] = INT_MAX;
    }

    // source vertex
    d[sourceVertex] = 0;
    struct frontier *fs = createFrontier();
    struct frontier *ns = createFrontier();

    for (int l = 0; l < INT_MAX; l++)
    {
        // add set of local vertices to fs
        int procArrayStart = floor((numVertices * rank) / worldSize);
        int procArrayLen = floor((numVertices * (rank + 1)) / worldSize) - procArrayStart;
        fs = createFrontier();

        for (int i = procArrayStart; i < procArrayStart + procArrayLen; i++)
        {
            if (d[i] == l)
            {
                enqueue(fs, i - procArrayStart);
            }
        }

        // termination of the main loop
        int fsize = fs->rear + 1;
        int check = 0;
        MPI_Allreduce(&fsize, &check, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

        if (check == 0)
        {
            break;
        }

        // find neighbors of vertices in fs
        // add neighbours to ns
        ns = createFrontier();
        for (int j = fs->front; !isEmpty(fs) && j < fs->rear + 1; j++)
        {
            for (int i = normalizedOffset[fs->values[j]]; i < normalizedOffset[fs->values[j] + 1]; i++)
            {
                enqueue(ns, locEdges[i]);
            }
        }

        // prepare buffers
        int sendBuff[worldSize][numVertices];
        int recvBuff[worldSize][numVertices];

        for (int i = 0; i < worldSize; i++)
        {
            for (int j = 0; j < numVertices; j++)
            {
                sendBuff[i][j] = -1;
                recvBuff[i][j] = -1;
            }
        }

        for (int i = 0; i < worldSize; i++)
        {
            int procArrayStart = floor((numVertices * i) / worldSize);
            int procArrayLen = floor((numVertices * (i + 1)) / worldSize) - procArrayStart;
            int count = 0;
            for (int j = ns->front; j < 1 + ns->rear; j++)
            {
                if ((ns->values[j] < (procArrayStart + procArrayLen)) && ns->values[j] >= procArrayStart)
                {
                    sendBuff[i][count] = ns->values[j];
                    ++count;
                }
            }
        }
        MPI_Barrier(MPI_COMM_WORLD);

        // all to all communication
        MPI_Alltoall((void *)&sendBuff, numVertices, MPI_INT, &recvBuff, numVertices, MPI_INT, MPI_COMM_WORLD);

        // get the data from receive buffers and update distances
        for (int i = 0; i < worldSize; i++)
        {
            int j = 0;
            while (j < numVertices && recvBuff[i][j] != -1)
            {
                if (d[recvBuff[i][j]] == INT_MAX)
                {
                    d[recvBuff[i][j]] = l + 1;
                }
                ++j;
            }
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    // SEND distances to root from all other processors
    if (rank != 0)
    {
        MPI_Send((void *)&d, numVertices, MPI_INT, 0, 0XA9FF, MPI_COMM_WORLD);
    }

    // in root combine distances by taking the maximum value for each index 
    if (rank == 0)
    {
        int dr[worldSize - 1][numVertices];

        for (int i = 1; i < worldSize; i++)
        {
            MPI_Recv(&dr[i - 1], numVertices, MPI_INT, i, 0XA9FF, MPI_COMM_WORLD, &s);
        }

        for (int i = 1; i < worldSize; i++)
        {
            for (int j = 0; j < numVertices; j++)
            {
                if (dr[i - 1][j] < d[j])
                {
                    d[j] = dr[i - 1][j];
                }
            }
        }

        // save to output file
        FILE *to = fopen(argv[4], "w");
        for (int i = 0; i < numVertices; i++)
        {
            if (d[i] == INT_MAX)
            {
                d[i] = -1;
            }
            fprintf(to, "%d\n", d[i]);
        }
    }
    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();
    return EXIT_SUCCESS;
}
