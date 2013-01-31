#include <mpi.h>
#include <stdlib.h>

#include "Visitor-MPI.h"

void Visitor_MPI_finalizeMPI() {{{
    MPI_Finalize();
}}}

void Visitor_MPI_getMPIInformation(int *i_am_supervisor, int *number_of_workers) {{{
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);
    *i_am_supervisor = (my_rank == 0);

    MPI_Comm_size(MPI_COMM_WORLD,number_of_workers);
    --(*number_of_workers);
}}}

void Visitor_MPI_initializeMPI() {{{
    MPI_Init(NULL,NULL);
}}}

void Visitor_MPI_receiveBroadcastMessage(char **message, int *size) {{{
    MPI_Bcast(size,1,MPI_INT,0,MPI_COMM_WORLD);
    *message = (char*)malloc((size_t)*size);
    MPI_Bcast(*message,*size,MPI_CHAR,0,MPI_COMM_WORLD);
}}}

void Visitor_MPI_sendBroadcastMessage(char *message, int size) {{{
    MPI_Bcast(&size,1,MPI_INT,0,MPI_COMM_WORLD);
    MPI_Bcast(message,size,MPI_CHAR,0,MPI_COMM_WORLD);
}}}

void Visitor_MPI_sendMessage(char *message, int size, int destination) {{{
    MPI_Send(message,size,MPI_CHAR,destination,0,MPI_COMM_WORLD);
}}}

void Visitor_MPI_tryReceiveMessage(int* source, char** message, int *size) {{{
    int flag;
    MPI_Status status;
    MPI_Iprobe(MPI_ANY_SOURCE,0,MPI_COMM_WORLD,&flag,&status);
    if(flag) {
        *source = status.MPI_SOURCE;
        MPI_Get_count(&status,MPI_CHAR,size);
        *message = (char*)malloc((size_t)*size);
        MPI_Recv(*message,*size,MPI_CHAR,*source,0,MPI_COMM_WORLD,MPI_STATUS_IGNORE);
    } else {
        *source = -1;
    }
}}}
