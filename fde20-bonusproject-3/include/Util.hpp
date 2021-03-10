#ifndef FDE20_BONUSPROJECT_3_UTIL_HPP
#define FDE20_BONUSPROJECT_3_UTIL_HPP

#include <string>
//---------------------------------------------------------------------------
inline std::string getDir(const std::string &file)
/// Returns path to file in 'file'
{
  size_t found = file.find_last_of("/\\");
  return (file.substr(0, found));
}
//---------------------------------------------------------------------------

#endif // FDE20_BONUSPROJECT_3_UTIL_HPP
