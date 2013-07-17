#ifndef VISITOR_MPI
#define VISITOR_MPI

void LogicGrowsOnTrees_MPI_finalizeMPI();
void LogicGrowsOnTrees_MPI_getMPIInformation(int *i_am_supervisor, int *number_of_workers);
void LogicGrowsOnTrees_MPI_initializeMPI();
void LogicGrowsOnTrees_MPI_receiveBroadcastMessage(char **message, int *size);
void LogicGrowsOnTrees_MPI_sendBroadcastMessage(char *message, int size);
void LogicGrowsOnTrees_MPI_sendMessage(char *message, int size, int destination);
void LogicGrowsOnTrees_MPI_tryReceiveMessage(int* source, char** message, int *size);

#endif
