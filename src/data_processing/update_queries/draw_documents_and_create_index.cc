#include <algorithm>
#include <cstring>
#include <deque>
#include <iostream>
#include <library/types.hh>
#include <random>

#include "data_processing/serializer/deserializer.hh"

// This program randomly draws 5% of the documents (from a documents file)
// and writes them in a separate file.
// Also, it re-creates the index file without the 5%.

enum class Dataset : u32 { CCNEWS, SSDB, TOY, TWITTER };

int main(int, char**) {
  // must be adjusted
  // TODO: parse CLI
  Dataset dataset = Dataset::SSDB;
  str prefix = "/mnt/dbgroup-share/mwidmoser/data/index/raw_index_and_document_files/";
  str binary_prefix = "/mnt/dbgroup-share/mwidmoser/data/index/serialized/";

  str documents_input_file;
  str documents_output_file;
  str original_index_binary_file;
  str index_output_file;
  u64 num_documents;
  u32 num_lists;

  switch(dataset) {
  case Dataset::CCNEWS: {
      documents_input_file = prefix + "ccnews-documents.txt";
      documents_output_file = prefix + "5p-random-documents-ccnews.txt";
      original_index_binary_file = binary_prefix + "cc-news-en.dat";
      index_output_file = prefix + "95p-ccnews-index.txt";
      num_documents = 43495426;
      num_lists = 29497691;
    break;

  } case Dataset::SSDB: {
    documents_input_file = prefix + "ssb-sf1-documents.txt";
    documents_output_file = prefix + "5p-random-documents-ssb-sf1.txt";
    original_index_binary_file = binary_prefix + "ssb-sf1.dat";
    index_output_file = prefix + "95p-ssb-sf1-index.txt";
    num_documents = 6001215 + 1;
    num_lists = 29;
    break;

  } case Dataset::TOY: {
      documents_input_file =
        prefix + "documents_data_uniform_2m_100k_50_100_200.txt";
      documents_output_file = "test-toy-insert-documents.txt";
      index_output_file = "test-95p-index-toy.txt";
      original_index_binary_file = binary_prefix + "bin_data_uniform_2m_100k_50_100_200.dat";
      num_documents = 2000000;
      num_lists = 100001;
      break;

  } case Dataset::TWITTER: {
      documents_input_file = prefix + "twitter-documents.txt";
      documents_output_file = "twitter-insert-documents.txt";
      original_index_binary_file = binary_prefix + "twitter-lists-reassigned-sorted.dat";
      index_output_file = prefix +
      "twitter-lists-reassigned-sorted-95p.txt";
      num_documents = 49395940;
      num_lists = 43983853;
      break;
  }
  }

  vec<vec<u32>> index(num_lists);

  std::cerr << "(slightly over-) pre-allocate the index.." << std::endl;
  {
    Deserializer d{original_index_binary_file};
    d.read_u32();  // universe size
    if (num_lists != d.read_u32()) {
      std::cerr << "num_lists does not match" << std::endl;
      std::exit(EXIT_FAILURE);
    }

    for (u64 i = 0; i < num_lists; ++i) {
      const u32 term = d.read_u32();
      const u32 list_size = d.read_u32();

      index[term].reserve(list_size);
      d.jump(list_size * sizeof(u32));
    }
  }

  const u32 num_samples = static_cast<u32>(0.05 * static_cast<f64>(num_documents));
  std::deque<u32> insert_documents;

  std::cerr << "sample documents..." << std::endl;
  {
    vec<u32> doc_ids(num_documents);
    std::iota(doc_ids.begin(), doc_ids.end(), 0);
    std::sample(doc_ids.begin(),
                doc_ids.end(),
                std::back_inserter(insert_documents),
                num_samples,
                std::mt19937{std::random_device{}()});
  }

  const auto popleft = [&]() -> u32 {
    const u32 next = insert_documents.front();
    insert_documents.pop_front();
    return next;
  };

  std::cerr << insert_documents.size() << " documents drawn" << std::endl;
  u32 next_doc = popleft();

  std::cerr << "reading documents..." << std::endl;

  std::ofstream dof;
  dof.open(documents_output_file, std::ios::out);

  std::ifstream dif;
  dif.open(documents_input_file, std::ios::in);

  u32 cnt = 0;

  if (dif.is_open()) {
    str line;
    while (std::getline(dif, line)) {
      if (++cnt % 10000 == 0) {
        std::cerr << static_cast<f64>(cnt) / num_documents * 100 << "%\n";
      }

      char* token = std::strtok(const_cast<char*>(line.c_str()), ":");
      const u32 doc_id = std::stoi(token);

      if (doc_id == next_doc) {
        dof << doc_id << ":";

        token = std::strtok(nullptr, " ");
        while (token != nullptr) {
          dof << " " << token;
          token = std::strtok(nullptr, " ");
        }
        dof << std::endl;

        if (!insert_documents.empty()) {
          next_doc = popleft();
        }
      } else {
        token = std::strtok(nullptr, " ");
        while (token != nullptr) {
          auto& list = index[std::stoi(token)];
          //          list.reserve(list.size() + 1);
          list.push_back(doc_id);
          token = std::strtok(nullptr, " ");
        }
      }
    }
  }

  dif.close();
  dof.close();

  std::cerr << "writing index..." << std::endl;
  u32 empty_lists = 0;

  std::ofstream f;
  f.open(index_output_file, std::ios::out);

  f << num_lists << std::endl;
  f << num_lists << std::endl;

  for (u32 term_id = 0; term_id < index.size(); ++term_id) {
    auto& list = index[term_id];

    if (!list.empty()) {
      f << term_id << ": ";
      for (u32 j = 0; j < list.size(); ++j) {
        f << list[j];
        if (j != list.size() - 1) {
          f << " ";
        }
      }
      f << "\n";
    } else {
      ++empty_lists;
    }
  }

  f.close();
  std::cerr << "empty lists: " << empty_lists << std::endl;

  return EXIT_SUCCESS;
}