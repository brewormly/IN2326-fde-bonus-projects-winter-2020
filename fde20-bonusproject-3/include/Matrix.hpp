#ifndef FDE20_BONUSPROJECT_3_MATRIX_HPP
#define FDE20_BONUSPROJECT_3_MATRIX_HPP

#include <cmath>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

//---------------------------------------------------------------------------
/// A sparse distance matrix representing a directed graph.
class Matrix {
  /// Store the matrix as list of entries.
  ///
  /// The entries are present in the file as: [row, column, weight]
  /// row: current node
  /// column: node to which we have an edge
  /// weight: weight of the edge
  ///
  /// The matrix stores the entries as two vectors (lists) to save space.
  /// entries: Only contains the column values
  /// entryOffsets: Contains the offsets of the rows
  /// We store the edges of the matrix therefore as follows:
  ///
  /// Input file:
  /// 1 4 7
  /// 4 6 1
  /// 5 6 5
  /// 6 2 4
  /// 6 4 5
  /// 7 1 9
  /// ...
  /// Stored matrix:
  /// entries: [column, weight]
  ///       0: [4, 7]
  ///       1: [6, 1]
  ///       2: [6, 5]
  ///       3: [2, 4]
  ///       4: [4, 5]
  ///       5: [1, 9]
  /// entryOffsets:
  ///       0: [0] -> no entry for row = 0
  ///       1: [1] -> one entry for row = 1
  ///       2: [1] -> no entry for row = 2
  ///       3: [1] -> no entry for row = 3
  ///       4: [2] -> one entry for row = 4
  ///       5: [3] -> one entry for row = 5
  ///       6: [5] -> two entries for row = 6
  ///       7: [6] -> one entry for row = 7
  ///
  /// You will only need the function getNeighbors() from the matrix

public:
  /// An entry
  struct Entry {
    /// The column
    unsigned column;
    /// The weight
    double weight;
    /// Constructor
    Entry(unsigned column, double weight) : column(column), weight(weight){};
    /// Needed for sorting.
    bool operator<(const Entry &obj) const {
      return this->weight < obj.weight;
    };
    /// Needed for equality checks
    bool operator==(const Entry &obj) const {
      return column == obj.column && std::fabs(weight - obj.weight) < 1E-3;
    }
    /// Print entry
    std::ostream &operator<<(std::ostream &stream) {
      return stream << "(Column: " << column << ", Weight: " << weight << ")";
    }
    friend std::ostream &operator<<(std::ostream &stream, const Entry &e) {
      return stream << "(Column: " << e.column << ", Weight: " << e.weight
                    << ")";
    }
  };

  /// An entry range
  struct EntryRange {
    /// The entry range
    const Entry *from, *to;

    /// Empty range?
    bool empty() const { return from == to; }

    /// First element
    const Entry *begin() { return from; }

    /// Behind the last element
    const Entry *end() { return to; }
  };

public:
  /// The entries of the matrix
  std::vector<Entry> entries;
  /// The entries offsets
  std::vector<unsigned> entryOffsets;
  /// The width of the matrix
  unsigned width = 0;

public:
  /// Get the number of rows
  unsigned getRowCount() const { return width; }

  /// This function returns all neighbors of node node (=nodes with outgoing edges
  /// from node to the neighbors). The function returns the neighbors as range
  /// containing all neighbor entries for the node node (= row node).
  /// To process the returned range, just iterate over it, for example as
  /// follows:
  /// for (auto &e : matrix.getNeighbors(currentNode)) {
  ///  ...handle the neighbor e...
  /// }
  EntryRange getNeighbors(unsigned node) const {
    return EntryRange{entries.data() + entryOffsets[node - 1],
                      entries.data() + entryOffsets[node]};
  }

  /// Read a sparse matrix file of a directed graph
  static Matrix readFile(std::string fileName) {
    // Check if we can read the file
    std::ifstream in(fileName);
    if (!in.is_open()) {
      std::cerr << "unable to read " << fileName << std::endl;
      exit(1);
    }

    // Matrix to store the elements
    Matrix result;

    // Check the header line
    std::string line;
    while (getline(in, line)) {
      if (line.empty() || (line.front() == '%'))
        continue;

      std::stringstream s(line);
      unsigned width, height, entries;
      s >> width >> height >> entries;
      if ((width != height) || (!width) || (!entries)) {
        std::cerr << "matrix must be symmetric and non-empty" << std::endl;
        exit(1);
      }
      result.width = width;
      result.entries.reserve(entries);
      result.entryOffsets.reserve(width + 1);
      break;
    }

    // Read the elements
    unsigned currentRow = 0, currentColumn = 0;
    while (getline(in, line)) {
      // Ignore comment lines
      if (line.empty() || (line.front() == '%'))
        continue;

      std::stringstream s(line);
      unsigned row, column;
      double weight;
      // Check if the line has the correct format
      if (!(s >> row >> column >> weight)) {
        std::cerr << "malformed matrix line " << line << std::endl;
        exit(1);
      }
      // Check if the row and column are in the matrix range
      if ((row > result.width) || (column > result.width)) {
        std::cerr << "malformed matrix format, cell offset out of bounds "
                  << line << std::endl;
        exit(1);
      }
      // Row numbers in the file have to be ascending
      if (row < currentRow) {
        std::cerr << "malformed matrix format, row number decreased " << line
                  << std::endl;
        exit(1);
      }
      // For each node the edges (columns) have to be ascending
      if (row == currentRow) {
        if (column <= currentColumn) {
          std::cerr << "malformed matrix format, column number decreased "
                    << line << std::endl;
          exit(1);
        }
        currentColumn = column; // Update the current column to the new one
      } else {
        // New row-node found, update the offset table
        result.entryOffsets.insert(result.entryOffsets.end(), row - currentRow,
                                   result.entries.size());
        // Update the current row & column
        currentRow = row;
        currentColumn = column;
      }
      // Insert the new entry
      result.entries.push_back({column, weight});
    }

    // Update the offset for the last element (-> end of matrix)
    result.entryOffsets.insert(result.entryOffsets.end(),
                               result.width + 1 - currentRow,
                               result.entries.size());

    return result;
  };
};
//---------------------------------------------------------------------------

#endif // FDE20_BONUSPROJECT_3_MATRIX_HPP
