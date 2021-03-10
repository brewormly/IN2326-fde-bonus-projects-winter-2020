#include "../include/KNN.hpp"
#include "../include/Util.hpp"
#include <gtest/gtest.h>

using namespace std;
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top1NearestNeighbor)
/// Test if we find even one neighbor
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 205;
    unsigned k = 1; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected = {{1874456, 0.60409999999999997}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top3NearestNeighbors)
/// Test if we find the top three neighbors
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 205;
    unsigned k = 3; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());

    std::vector<Matrix::Entry> expected = {{1874456, 0.60409999999999997},
                                           {1552034, 1.3064519999999999},
                                           {1901000, 1.510168}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top10NotReachable)
/// Test if we handle the case where the #reachable neighbors is smaller then k.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 205;
    unsigned k = 10; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());

    std::sort(result.begin(), result.end());

    // We can find at most 7 neighbors, although we search for 10
    std::vector<Matrix::Entry> expected{{1874456, 0.6041},  {1552034, 1.30645},
                                        {1901000, 1.51017}, {1836251, 1.97767},
                                        {569161, 2.29636},  {572469, 2.78174},
                                        {1253738, 3.14601}};
    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top10NearestNeighbors)
/// Test if we find the correct top 10 neighbors.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 210;
    unsigned k = 10; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected{{764770, 0.664075}, {141882, 0.768491},
                                        {1666237, 1.27067}, {1753834, 1.5857},
                                        {107535, 1.60894},  {460877, 1.76713},
                                        {172760, 2.0208},   {1171827, 2.2021},
                                        {1139888, 2.34425}, {390238, 2.488}};
    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top20NearestNeighbors)
/// Test if we find the correct top 20 neighbors.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 210;
    unsigned k = 20; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected{
            {764770, 0.664075}, {141882, 0.768491}, {1666237, 1.27067},
            {1753834, 1.5857},  {107535, 1.60894},  {460877, 1.76713},
            {172760, 2.0208},   {1171827, 2.2021},  {1139888, 2.34425},
            {390238, 2.488},    {1009041, 3.12687}, {864881, 3.19292},
            {589617, 3.39672},  {82882, 3.76808},   {712693, 3.9045},
            {88886, 4.06643},   {942076, 4.15386},  {860126, 4.16168},
            {1965705, 4.77789}, {315270, 4.79742}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top30NearestNeighbors)
/// Test if we find the correct top 30 neighbors.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 210;
    unsigned k = 30; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected{
            {764770, 0.664075}, {141882, 0.768491}, {1666237, 1.27067},
            {1753834, 1.5857},  {107535, 1.60894},  {460877, 1.76713},
            {172760, 2.0208},   {1171827, 2.2021},  {1139888, 2.34425},
            {390238, 2.488},    {1009041, 3.12687}, {864881, 3.19292},
            {589617, 3.39672},  {82882, 3.76808},   {712693, 3.9045},
            {88886, 4.06643},   {942076, 4.15386},  {860126, 4.16168},
            {1965705, 4.77789}, {315270, 4.79742},  {1390872, 5.27184},
            {558899, 5.40213},  {1752015, 5.62945}, {246118, 5.64652},
            {1427109, 5.94046}, {88693, 5.99056},   {1228680, 6.03966},
            {492662, 6.06492},  {1701681, 6.55891}, {1037304, 6.91}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top50NearestNeighbors)
/// Test if we find the correct top 50 neighbors.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 210;
    unsigned k = 50; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected{
            {764770, 0.664075}, {141882, 0.768491}, {1666237, 1.27067},
            {1753834, 1.5857},  {107535, 1.60894},  {460877, 1.76713},
            {172760, 2.0208},   {1171827, 2.2021},  {1139888, 2.34425},
            {390238, 2.488},    {1009041, 3.12687}, {864881, 3.19292},
            {589617, 3.39672},  {82882, 3.76808},   {712693, 3.9045},
            {88886, 4.06643},   {942076, 4.15386},  {860126, 4.16168},
            {1965705, 4.77789}, {315270, 4.79742},  {1390872, 5.27184},
            {558899, 5.40213},  {1752015, 5.62945}, {246118, 5.64652},
            {1427109, 5.94046}, {88693, 5.99056},   {1228680, 6.03966},
            {492662, 6.06492},  {1701681, 6.55891}, {1037304, 6.91},
            {709873, 6.92481},  {732409, 7.08362},  {582309, 7.09062},
            {1102864, 7.15872}, {1012016, 7.21921}, {1252718, 7.23182},
            {145385, 7.2423},   {7581, 7.24408},    {321954, 7.24432},
            {289315, 7.52459},  {1704936, 7.57057}, {1435921, 7.66179},
            {1993573, 7.69467}, {996893, 7.89425},  {462638, 7.94804},
            {1181037, 7.98779}, {1385587, 7.99686}, {1689521, 8.0248},
            {1874903, 8.04532}, {290355, 8.25242}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
//---------------------------------------------------------------------------
TEST(KNearestNeighbors, Top100NearestNeighbors)
/// Test if we find the correct top 100 neighbors.
{
    // Read the matrix file and store it as internal matrix.
    auto matrix_file = getDir(__FILE__) + "/data/dataSmall.mtx";
    Matrix matrix = Matrix::readFile(matrix_file);

    unsigned start_node = 210;
    unsigned k = 100; // The number of neighbors we are interested in.

    auto result = getKNN(matrix, start_node, k);

    std::sort(result.begin(), result.end());
    std::vector<Matrix::Entry> expected{
            {764770, 0.664075}, {141882, 0.768491}, {1666237, 1.27067},
            {1753834, 1.5857},  {107535, 1.60894},  {460877, 1.76713},
            {172760, 2.0208},   {1171827, 2.2021},  {1139888, 2.34425},
            {390238, 2.488},    {1009041, 3.12687}, {864881, 3.19292},
            {589617, 3.39672},  {82882, 3.76808},   {712693, 3.9045},
            {88886, 4.06643},   {942076, 4.15386},  {860126, 4.16168},
            {1965705, 4.77789}, {315270, 4.79742},  {1390872, 5.27184},
            {558899, 5.40213},  {1752015, 5.62945}, {246118, 5.64652},
            {1427109, 5.94046}, {88693, 5.99056},   {1228680, 6.03966},
            {492662, 6.06492},  {1701681, 6.55891}, {1037304, 6.91},
            {709873, 6.92481},  {732409, 7.08362},  {582309, 7.09062},
            {1102864, 7.15872}, {1012016, 7.21921}, {1252718, 7.23182},
            {145385, 7.2423},   {7581, 7.24408},    {321954, 7.24432},
            {289315, 7.52459},  {1704936, 7.57057}, {1435921, 7.66179},
            {1993573, 7.69467}, {996893, 7.89425},  {462638, 7.94804},
            {1181037, 7.98779}, {1385587, 7.99686}, {1689521, 8.0248},
            {1874903, 8.04532}, {290355, 8.25242},  {1087049, 8.26946},
            {33778, 8.31356},   {837520, 8.37517},  {519549, 8.38359},
            {1209821, 8.40718}, {1052190, 8.4257},  {279645, 8.50045},
            {347928, 8.54932},  {974685, 8.59171},  {1194601, 8.62871},
            {362310, 8.72822},  {1836854, 8.76168}, {1819335, 8.84111},
            {519025, 9.09717},  {948036, 9.14629},  {94509, 9.15392},
            {45112, 9.26206},   {1331872, 9.26721}, {1071392, 9.36021},
            {653396, 9.40626},  {124503, 9.46021},  {530001, 9.49918},
            {18262, 9.59804},   {133240, 9.59872},  {97593, 9.73162},
            {1368833, 9.85703}, {1920494, 9.89841}, {1136914, 9.91},
            {80263, 9.92795},   {1858061, 9.94787}, {1307293, 9.99909},
            {1598671, 10.1668}, {207041, 10.3269},  {70921, 10.372},
            {1851695, 10.4148}, {1571752, 10.5047}, {1542589, 10.5344},
            {834520, 10.5587},  {1511889, 10.6783}, {340125, 10.8356},
            {472459, 10.8938},  {931550, 10.9064},  {64623, 10.9258},
            {539122, 11.0281},  {304560, 11.3641},  {1241099, 11.4687},
            {1850405, 11.5275}, {1351118, 11.5455}, {1566344, 11.5897},
            {720881, 11.7335}};

    ASSERT_EQ(result.size(), expected.size())
                                << "Result does not contain correct number of elements.";

    for (size_t i = 0; i < result.size(); ++i) {
        ASSERT_EQ(result[i], expected[i])
                                    << "Result and expected result differ at index " << i << endl
                                    << "result[i]: " << result[i] << endl
                                    << "expected[i]: " << expected[i];
    }
}
