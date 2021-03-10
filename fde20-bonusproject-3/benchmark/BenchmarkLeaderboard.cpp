#include "../include/Matrix.hpp"
#include "../include/KNN.hpp"
#include <iostream>
#include <vector>

using namespace std;
//---------------------------------------------------------------------------
int main(int argc, char *argv[]) {
  if (argc != 2) {
    cout << "usage: " << argv[0] << " <matrixfile>" << endl;
    return 0;
  }

  // Read the matrix file and store it as internal matrix.
  Matrix matrix = Matrix::readFile(argv[1]);
  // Store the parameters for the different runs
  vector<pair<unsigned, unsigned>> params;
  // Read from the standard input: <start node> <knn>
  for (string line; getline(cin, line);) {
    // Find position to split the string
    auto whitespace = line.find(' ');
    // The start node
    unsigned start = atoll(line.substr(0, whitespace).c_str());
    // The number of neighbors we are interested in.
    unsigned k = atoll(line.substr(whitespace, line.length()).c_str());
    params.push_back({start, k});
  }

  // Computes the sum of all the weights, just to have some result to show.
  auto computeSum = [](vector<Matrix::Entry> res) {
    double sum = 0.0;
    for (auto e : res)
      sum += e.weight;
    return sum;
  };

  // Execute the algorithm for all nodes and parameters
  for (auto &param : params)
        cout << computeSum(getKNN(matrix, param.first, param.second)) << "\n";

  // Flush output buffer
  cout.flush();
}
//---------------------------------------------------------------------------
