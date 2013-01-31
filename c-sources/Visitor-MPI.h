#ifndef VISITOR_MPI
#define VISITOR_MPI

void Visitor_MPI_finalizeMPI();
void Visitor_MPI_getMPIInformation(int *i_am_supervisor, int *number_of_workers);
void Visitor_MPI_initializeMPI();
void Visitor_MPI_receiveBroadcastMessage(char **message, int *size);
void Visitor_MPI_sendBroadcastMessage(char *message, int size);
void Visitor_MPI_sendMessage(char *message, int size, int destination);
void Visitor_MPI_tryReceiveMessage(int* source, char** message, int *size);

#endif
