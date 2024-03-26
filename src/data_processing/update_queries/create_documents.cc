#include <iostream>
#include <library/types.hh>

#include "data_processing/serializer/deserializer.hh"

// This program re-creates documents out of a binary index file
// in the following format: document_id: term1 term2 ...
// Output is written to stdout (hence, > it into a file)
// The filename should be prefix + "raw_index_and_document_files/dataset-documents.txt"

enum class Dataset : u32 { CCNEWS, SSDB, TWITTER };

int main(int, char**) {
  // must be adjusted
  // TODO: parse CLI
  Dataset dataset = Dataset::SSDB;
  str prefix = "/mnt/dbgroup-share/mwidmoser/data/";

  str binary_index;
  u64 num_documents;

  switch(dataset) {
  case Dataset::CCNEWS: {
    binary_index = prefix + "index/serialized/cc-news-en.dat";
    num_documents = 43495426;
    break;
  } case Dataset::SSDB: {
    binary_index = prefix + "index/serialized/ssb-sf1.dat";
    num_documents = 6001215 + 1;
    break;
  } case Dataset::TWITTER: {
    binary_index = prefix + "index/serialized/twitter-lists-reassigned-sorted.dat";
    num_documents = 49395939 + 1;
    break;
  }
  }

  vec<vec<u32>> documents(num_documents);

  std::cerr << "reading index..." << std::endl;
  Deserializer d{binary_index};

  const u32 universe_size = d.read_u32();
  const u32 num_lists = d.read_u32();

  std::cerr << "universe size: " << universe_size << std::endl;
  std::cerr << "num lists: " << num_lists << std::endl;

  for (u64 i = 0; i < num_lists; ++i) {
    const u32 term = d.read_u32();
    const u32 list_size = d.read_u32();

    if (i % 10000 == 0) {
      std::cerr << static_cast<f64>(i) / num_lists * 100 << "% ...\n";
    }

    for (u32 j = 0; j < list_size; ++j) {
      const u32 doc_id = d.read_u32();
      auto& list = documents[doc_id];

      // only necessary if there is not enough RAM
      if (dataset == Dataset::CCNEWS) {
        list.reserve(list.size() + 1);
      }

      list.push_back(term);
    }
  }

  std::cerr << "writing documents..." << std::endl;
  for (size_t i = 0; i < documents.size(); ++i) {
    std::cout << i << ": ";
    auto& document = documents[i];

    for (u32 t : document) {
      std::cout << t << " ";
    }
    std::cout << std::endl;
  }

  return EXIT_SUCCESS;
}
