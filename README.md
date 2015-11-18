
# pageRankAlgorithmErlang

Erlang program that execute the pageRank Algorithm

Main Function

pageRank(MatrixFileName, N, K, Iterations, Beta, Nodes)

MatrixFileName = Name of the file that contains the description of the pages and their connections with other pages.

N = The number of pages on the matrix file.

K = The number of blocks to divide the matrix

Iterations = The number of iterations of the algorithm

Beta = The number beta of the algorithm

Nodes = The nodes to compute the algorithm

1) First compile the module with the next command

c(pageRank).

2) After compile to execute the function type the next command:

pageRank.pageRank('matrixFile.csv', 8, 2, 5, 0.8, {'nodeA@mcalvo', 'nodeB@mcalvo'}).

