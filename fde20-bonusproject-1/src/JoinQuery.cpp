#include "JoinQuery.hpp"
#include <assert.h>
#include <fstream>
#include <thread>

#include <fcntl.h>
#include <immintrin.h>
#include <sys/mman.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>
//an unordered set is better than a vector...
#include <unordered_set>

static auto openFile(const char *fileName)
{
   // from the lecture we know this fancy stuff
   int handle = open(fileName, O_RDONLY);
   lseek(handle, 0, SEEK_END);
   auto length = lseek(handle, 0, SEEK_CUR);
   void *data = mmap(nullptr, length, PROT_READ, MAP_SHARED, handle, 0);
   auto begin = static_cast<const char *>(data), end = begin + length;

   //segmentation fault
   //::close(handle);

   return std::make_pair(begin, end);
}

/**
 * This fancy stuff is copyright by Prof. Neumann and
 * was taught during the FDE lecture WS 2020.
 */

template <char separator>
static const char *findPattern(const char *iter, const char *end)
// Returns the position after the pattern within [iter, end[, or end if not
// found
{
   // Loop over the content in blocks of 32 characters
   auto end32 = end - 32;
   const auto pattern = _mm256_set1_epi8(separator);
   for (; iter < end32; iter += 32) {
      // Check the next 32 characters for the pattern
      auto block = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(iter));
      auto matches = _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, pattern));
      if (matches) return iter + __builtin_ctzll(matches) + 1;
   }

   // Check the last few characters explicitly
   while ((iter < end) && ((*iter) != separator)) ++iter;
   if (iter < end) ++iter;
   return iter;
}

static constexpr uint64_t buildPattern(char c) {
   // Convert 00000000000000CC -> CCCCCCCCCCCCCCCC
   uint64_t v = c;
   return (v << 56 | v << 48 | v << 40 | v << 32 | v << 24 | v << 16 | v << 8) |
          v;
}

template <char separator>
static const char *findNthPattern(const char *iter, const char *end, unsigned n)
// Returns the position after the pattern within [iter, end[, or end if not
// found
{
   // Loop over the content in blocks of 8 characters
   auto end8 = end - 8;
   constexpr uint64_t pattern = buildPattern(separator);
   for (; iter < end8; iter += 8) {
      // Check the next 8 characters for the pattern
      uint64_t block = *reinterpret_cast<const uint64_t *>(iter);
      constexpr uint64_t high = 0x8080808080808080ull;
      constexpr uint64_t low = ~high;
      uint64_t lowChars = (~block) & high;
      uint64_t foundPattern = ~((((block & low) ^ pattern) + low) & high);
      uint64_t matches = foundPattern & lowChars;
      if (matches) {
         unsigned hits = __builtin_popcountll(matches);
         if (hits >= n) {
            for (; n > 1; n--)
               matches &= matches - 1;
            return iter + (__builtin_ctzll(matches) >> 3) + 1;
         }
         n -= hits;
      }
   }

   // Check the last few characters explicitly
   for (; iter < end; ++iter)
      if ((*iter) == separator)
         if ((--n) == 0)
            return iter + 1;

   return end;
}
/**
 * End of the copyrighted cpp madness
 */

static std::unordered_set<std::string> extractOrders(const char *beginOfFile,
                                                 const char *endOfFile,
                                                 std::unordered_set<std::string> orderIds)
{
   std::unordered_set<std::string> result;
   std::string orderkey = "";
   std::string custkey = "";
   for (auto iterator = beginOfFile, end = endOfFile; iterator != end; ) {
      orderkey = "";
      custkey = "";

      //build the orderkey hopefully fast
      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|') break;
         orderkey += (*iterator);
      }

      ++iterator;

      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|')
            break;
         custkey += (*iterator);
      }
      //std::find is a bad boy
      //std::find(orderIds.begin(), orderIds.end(), custkey) != orderIds.end()
      auto searchResult = orderIds.find(custkey);
      if(searchResult != orderIds.end())
         result.insert(orderkey);

      iterator = findPattern<'\n'>(iterator, end);
   }

   return result;
}

static std::unordered_set<std::string> extractCustomers(const char *beginOfFile,
                                                             const char *endOfFile,
                                                             std::string segmentParam)
{
   std::unordered_set<std::string> result;
   std::string mktsegment = "";
   std::string custkey = "";
   for (auto iterator = beginOfFile, end = endOfFile; iterator != end; ) {
      mktsegment = "";
      custkey = "";

      //build the custkey hopefully fast
      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|') break;
         custkey += (*iterator);
      }
      //find the mktsegment and go to beginning of it
      iterator = findNthPattern<'|'>(iterator, end, 6);

      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|')
            break;
         mktsegment += (*iterator);
      }
      // unordered_set will do the trick!
      if(mktsegment == segmentParam)
         result.insert(custkey);

      iterator = findPattern<'\n'>(iterator, end);
   }

   return result;
}

static uint64_t extractLineItemQuantities(const char *beginOfFile,
                                      const char *endOfFile,
                                          std::unordered_set<std::string> orderIds)
{
   unsigned counter = 0;
   unsigned sum = 0;
   std::string orderkey;
   std::string quantity;
   for (auto iterator = beginOfFile, end = endOfFile; iterator != end; ) {
      orderkey = "";
      quantity = "";

      //build the orderkey hopefully fast
      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|') break;
         orderkey += (*iterator);
      }
      auto searchResult = orderIds.find(orderkey);
      bool test = false;
      if(searchResult != orderIds.end())
         test = true;

      iterator = findNthPattern<'|'>(iterator, end, 4);

      for (; iterator != end; ++iterator) {
         char c =* iterator;
         if (c == '|')
            break;
         quantity += (*iterator);
      }
      if(test){
         sum += std::stoi(quantity);
         counter++;
      }

      iterator = findPattern<'\n'>(iterator, end);
   }

   return sum * 100  / counter;
}


static std::pair<const char *, const char *> lineitemFile;
static std::pair<const char *, const char *> orderFile;
static std::pair<const char *, const char *> customerFile;

//---------------------------------------------------------------------------
JoinQuery::JoinQuery(std::string lineitem, std::string order,
                     std::string customer)
{
   lineitemFile = openFile(lineitem.c_str());
   orderFile = openFile(order.c_str());
   customerFile = openFile(customer.c_str());
}
//---------------------------------------------------------------------------
size_t JoinQuery::avg(std::string segmentParam)
{
   // instead of std::vector we use now std::unordered_map because the find function should be faster...
   auto customerIds = extractCustomers(customerFile.first, customerFile.second, segmentParam);
   auto orderIds = extractOrders(orderFile.first, orderFile.second, customerIds);

   auto avg = extractLineItemQuantities(lineitemFile.first, lineitemFile.second, orderIds);

   //segmentation fault
   //munmap((void *)customerFile.first, customerFile.second - customerFile.first);
   //munmap((void *)orderFile.first, orderFile.second - orderFile.first);
   //munmap((void *)lineitemFile.first, lineitemFile.second - lineitemFile.first);

   return avg;
}
//---------------------------------------------------------------------------
size_t JoinQuery::lineCount(std::string rel)
{
   std::ifstream relation(rel);
   assert(relation);  // make sure the provided string references a file
   size_t n = 0;
   for (std::string line; std::getline(relation, line);) n++;
   return n;
}
//---------------------------------------------------------------------------
