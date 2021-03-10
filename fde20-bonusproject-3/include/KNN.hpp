#ifndef FDE20_BONUSPROJECT_3_KNN_HPP
#define FDE20_BONUSPROJECT_3_KNN_HPP

#include "Matrix.hpp"
//my includes:
#include <unordered_set>
#include <map>
#include <set>

struct compare
{
    bool operator()(const Matrix::Entry& l, const Matrix::Entry& r)
    {
        return l.weight > r.weight;
    }
};

/**
 * I don't want to change something in the Matrix.hpp, so here is the comparator.
 * Thanks to Hannes I now know that the test on the benchmark server likes to
 * compare also the column...
 */
struct compare_map
{
    bool operator()(const Matrix::Entry& l, const Matrix::Entry& r)
    const {
        if(l.weight == r.weight){
            return l.column < r.column;
        }
        return l.weight < r.weight;
    }
};

/*
 * On the first glance, I thought that a priority queue would be cool.
 * But a map is also really fast.
 * template<
        class T,
        class Container = std::vector<T>,
        class Compare = std::less<typename Container::value_type>
> class MyQueue : public std::priority_queue<T, Container, Compare>
{
public:
    typedef typename
    std::priority_queue<
            T,
            Container,
            Compare>::container_type::const_iterator const_iterator;

    const_iterator find(const T&val) const
    {
        auto first = this->c.cbegin();
        auto last = this->c.cend();
        while (first!=last) {
            if (*first==val) return first;
            ++first;
        }
        return last;
    }
    const_iterator end() const{
        return this->c.end();
    }
};*/

//---------------------------------------------------------------------------
/// Find the top k neighbors for the node start. The directed graph is stored in
/// matrix m and you can use getNeighbors(node) to get all neighbors of a node.
/// A more detailed description is provided in Matrix.hpp.
/// The function should return the k nearest neighbors in sorted order as vector
/// of Matrix::Entry, where Entry->column is the neighbor node and Entry->weight
/// is the cost to reach the node from the start node.
std::vector<Matrix::Entry> getKNN(const Matrix &m, unsigned start, unsigned k) {
    using Entry = Matrix::Entry;
    std::vector<Entry> result;
    result.reserve(k);

    std::map<Entry, bool, compare_map> rankedNodes;
    rankedNodes[Entry(start, 0.0)]=  false;
    // in the beginning there is always something to discover :)
    bool thereIsSomethingToDiscover = true;

    // I have read that a set is sometimes faster than a unordered_set
    std::set<unsigned> visitedNodes;

    while(thereIsSomethingToDiscover) {
        // now I am getting used to this old-fashioned cpp programming...
        int toDiscover = -1;
        double toDiscover_weight = -1.0;
        for (auto const &x : rankedNodes) {
            if (!x.second) {
                toDiscover = x.first.column;
                toDiscover_weight = x.first.weight;
                break;
            }
        }
        if (toDiscover == -1) {
            thereIsSomethingToDiscover = false;
            break;
        }
        rankedNodes[Entry(toDiscover, toDiscover_weight)] = true;
        // walk to the neighbors like its 1999
        for (auto &e : m.getNeighbors(toDiscover)) {
            auto searchResult = visitedNodes.find(e.column);
            if (searchResult == visitedNodes.end()) {
                int column = e.column;
                double weight = e.weight;
                double calcedWeight = toDiscover_weight + weight;
                if(calcedWeight < (--rankedNodes.end())->first.weight || rankedNodes.size() < k + 1 ){
                    rankedNodes[Entry(column, calcedWeight)] = false;
                }

                if (rankedNodes.size() > k + 1) {
                    auto end = rankedNodes.end();
                    rankedNodes.erase(--end);
                }
            }
        }
        visitedNodes.insert(toDiscover);
    }

    rankedNodes.erase(rankedNodes.begin());
    for( auto it = rankedNodes.begin(); it != rankedNodes.end(); ++it ) {
        result.push_back( it->first );
    }

    return result;
}

//---------------------------------------------------------------------------

#endif // FDE20_BONUSPROJECT_3_KNN_HPP