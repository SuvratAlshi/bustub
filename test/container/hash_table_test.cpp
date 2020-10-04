//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_test.cpp
//
// Identification: test/container/hash_table_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/logger.h"
#include "container/hash/linear_probe_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

TEST(HashTableTest, ResizeSimpleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  EXPECT_EQ(1000, ht.GetSize());
  // bool found = false;
  ht.Insert(nullptr, 73, 676);
  std::vector<int> res;
  ht.GetValue(nullptr, 73, &res);
  EXPECT_EQ(676, res[0]);
  for (auto x : res){
    std::cout << x << std::endl;
  }
 
  ht.Resize(3000);

  res.clear();
  ht.GetValue(nullptr, 73, &res);
  EXPECT_EQ(676, res[0]);
  for (auto x : res){
    std::cout << x << std::endl;
  }

  EXPECT_EQ(6000, ht.GetSize());

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ResizeIteratorTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  EXPECT_EQ(1000, ht.GetSize());

  // insert a few values
  for (int i = 0; i < 500; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 300; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
 
  ht.Resize(3000);
  EXPECT_EQ(6000, ht.GetSize());

  // check if the inserted values are all there
  for (int i = 0; i < 300; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, ResizeFullFillTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  EXPECT_EQ(1000, ht.GetSize());

  // insert a few values
  for (int i = 0; i < 1500; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 1500; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }
 
  EXPECT_EQ(2000, ht.GetSize());

  // check if the inserted values are all there
  for (int i = 0; i < 1500; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

TEST(HashTableTest, BasicTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // bool found = false;
  ht.Insert(nullptr, 73, 676);
  std::cout << "Size of the hash table is " << ht.GetSize() << std::endl;
  std::vector<int> res;
  ht.GetValue(nullptr, 73, &res);
  std::cout << "Printing out the result vector, size of vector is " << res.size() << std::endl;
  for (auto x : res){
    std::cout << x << std::endl;
  }
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
  std::cout << "End of basic hash table test" << std::endl;
}

// NOLINTNEXTLINE
TEST(HashTableTest, SampleTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManager(50, disk_manager);

  LinearProbeHashTable<int, int, IntComparator> ht("blah", bpm, IntComparator(), 1000, HashFunction<int>());

  // insert a few values
  for (int i = 0; i < 5; i++) {
    ht.Insert(nullptr, i, i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to insert " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // check if the inserted values are all there
  for (int i = 0; i < 5; i++) {
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    EXPECT_EQ(1, res.size()) << "Failed to keep " << i << std::endl;
    EXPECT_EQ(i, res[0]);
  }

  // insert one more value for each key
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_FALSE(ht.Insert(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Insert(nullptr, i, 2 * i));
    }
    ht.Insert(nullptr, i, 2 * i);
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // duplicate values for the same key are not allowed
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(i, res[0]);
    } else {
      EXPECT_EQ(2, res.size());
      if (res[0] == i) {
        EXPECT_EQ(2 * i, res[1]);
      } else {
        EXPECT_EQ(2 * i, res[0]);
        EXPECT_EQ(i, res[1]);
      }
    }
  }

  // look for a key that does not exist
  std::vector<int> res;
  ht.GetValue(nullptr, 20, &res);
  EXPECT_EQ(0, res.size());

  // delete some values
  for (int i = 0; i < 5; i++) {
    EXPECT_TRUE(ht.Remove(nullptr, i, i));
    std::vector<int> res;
    ht.GetValue(nullptr, i, &res);
    if (i == 0) {
      // (0, 0) is the only pair with key 0
      EXPECT_EQ(0, res.size());
    } else {
      EXPECT_EQ(1, res.size());
      EXPECT_EQ(2 * i, res[0]);
    }
  }

  // delete all values
  for (int i = 0; i < 5; i++) {
    if (i == 0) {
      // (0, 0) has been deleted
      EXPECT_FALSE(ht.Remove(nullptr, i, 2 * i));
    } else {
      EXPECT_TRUE(ht.Remove(nullptr, i, 2 * i));
    }
  }
  disk_manager->ShutDown();
  remove("test.db");
  delete disk_manager;
  delete bpm;
}

}  // namespace bustub
