#include "../include/KNN.hpp"
#include <iostream>
#include <vector>

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  // Insert the matrix file as absolute path!
  // The path must not contain whitespaces!
  // Usage example:
  // forTesting Users/fde/CLionProjects/fde20-bonusproject-3/test/data/dataSmall.mtx 25 90
  if (argc != 4) {
    cout << "usage: " << argv[0] << " <matrixfile> "
         << "<start_node> "
         << "<k_nearest_neighbors>" << endl;
    return 0;
  }

  // Read the matrix file and store it as internal matrix.
  Matrix matrix = Matrix::readFile(argv[1]);
  // The start node
  unsigned start = atoll(argv[2]);
  // The number of neighbors we are interested in.
  unsigned k = atoll(argv[3]);

  // Find the top k elements for the relevant node.
  auto neighbors = getKNN(matrix, start, k);

  cout << "Result contains " << neighbors.size() << " elements." << endl;

  for(auto e: neighbors)
    cout << e << ", ";
  cout << endl;

  // Compute the sum to check correctness
  double sum = 0;
  for (auto &n : neighbors)
    sum += n.weight;

  cout << sum;
  cout.flush();
  return 1;
}
//---------------------------------------------------------------------------
