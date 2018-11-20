/*
 * Copyright (C) 2018 DataJaguar, Inc.
 *
 * This file is part of JaguarDB.
 *
 * JaguarDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JaguarDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JaguarDB (LICENSE.txt). If not, see <http://www.gnu.org/licenses/>.
 */
#pragma once
#include <stdio.h>
#include <JagCfg.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <list>
#include <map>
#include <vector>
#include <queue>
#include <functional>
#include <algorithm>
#include <unordered_map>
#include <tuple>
#include <ctime>
#include <JagRecord.h>
#include <JagSchemaRecord.h>
#include <JagParser.h>
#include <abax.h>
#include <JagTableLine.h>
#include <JagTableLineCompare.h>

#define EXSORT
#define BUFF_LEN 8
#define FORMAT_STR "%08d"
#define MAIN_BUFF_LEN 100000000

class JagTableExSort {

#ifdef EXSORT
	public:
	/**
	 * Use to external sort. filename is name of the file you want to sort. record is the
	 * schema of the input file. parseParam are the parameters the client wants to sort
	 * by. outfile is the output file that is created/overwritten.
	 */
	void externalSort( const JagParseParam *parseParam, const JagSchemaRecord& schema, 
					   const AbaxDataString& input, const AbaxDataString& output, 
					   bool group_by = false, bool descending = false) 
    {

		std::cout << "External Sort started" << std::endl;
		const char *c_name = input.c_str();		// C string of the input file
		std::vector<AbaxDataString> sort_by;		// Stores the sort parameters
		AbaxDataString sort_param;			// String to store the sort
		int is_key;

		if(group_by) {
			for(int i = 0; i < parseParam->groupVec.size(); i++) {
				// Check if it is a key
				is_key = JagTableLine::getKeyCol(parseParam->groupVec[i].colName, schema);
				if(is_key != -1) {
					//sort_param = parseParam->groupVec[i].dbName + "." + 
					// parseParam->groupVec[i].tableName + "." + parseParam->groupVec[i].colName;
					sort_param = parseParam->groupVec[i].colName;
				} else {
					sort_param = parseParam->groupVec[i].colName;
				}

				sort_by.push_back(sort_param);
			}
		}
		else {
			for(int i = 0; i < parseParam->orderVec.size(); i++) {
				// Check if it is a key
				is_key = JagTableLine::getKeyCol(parseParam->orderVec[i].colName, schema);
				if(is_key != -1) {
					//sort_param = parseParam->orderVec[i].dbName + "." + 
					// parseParam->orderVec[i].tableName + "." + parseParam->orderVec[i].colName;
					sort_param = parseParam->groupVec[i].colName;
				}
				else
					sort_param = parseParam->orderVec[i].colName;

				sort_by.push_back(sort_param);
			}
		}

		std::ifstream read_buff;			// stream to read files from
		std::ofstream write_file;			// stream to write files to
		int boxNum = 0;					// # of boxes
		int lineNum = 0;				// Counter for lines per box
		std::vector<AbaxDataString> box_vec;		// stores list of names of temp files
		read_buff.open(c_name, std::ios::in);		// Open the input file for reading

		// Check if it is empty
		if(is_empty(read_buff)) {
			read_buff.close();
			printf("File is empty");
			return;
		}

		unsigned int numCPU = sysconf(_SC_NPROCESSORS_ONLN);
		std::cout << "# threads: " << numCPU << std::endl;
		abaxint fileSize = file_size(input);				// Size of input file
		int max_buf = get_max_buf();					// Maximum buffer size
		int num_box = (int)ceil((double)fileSize/max_buf) * numCPU;	// # of temp files to use
		abaxint char_per_file = (abaxint)ceil((double)fileSize/num_box);
		int hash_num = (int)hash(input.c_str());			// Hash the input file to determine box file names
		std::vector<std::vector<JagTableLine>> file_vec;

		// Create all the temp files
		for(int i = 0; i < num_box; i++) {
			// Create a new file for the first split file and store in vector
			// Name is based off a hash_function of the original input file name
			write_file.open(to_string(hash_num + i) + ".dat", std::ios::out);
			write_file.close();
			box_vec.push_back(to_string(hash_num + i) +".dat");
			std::vector<JagTableLine> line_vec;
			file_vec.push_back(line_vec);
		}

		int file_open = 0;
		std::cout << "# of temp files: " << num_box << std::endl;
		int rr_length;				// Length of the actual ray record(Will be determined by 8 bytes)
		abaxint char_read;				// # of characters read so far
		int rr_count = 0;			// # of ray records in total
		char lenbuf[BUFF_LEN + 1];		// char array to store the 8 bytes

		// Create the vectors to store the lines in
		for(abaxint i = 0; i < file_vec.size(); i++) {
			char_read = 0;			// Reset characters read to 0
			while(char_read < char_per_file && !is_empty(read_buff)) {
				read_buff.read(lenbuf, BUFF_LEN);	// read the 8 characters in
				rr_length = atol(lenbuf);		// convert the string to a number
				char_read += rr_length + BUFF_LEN;
				char line[rr_length + 1];
				read_buff.read(line, rr_length);
				JagTableLine jag_line(line);
				file_vec[i].push_back(jag_line);
				rr_count++;
			}

			std::cout << file_vec[i].size() << std::endl;
		}

		std::cout << "# of RR: " << rr_count << std::endl;
		
		for(int i = 0; i < file_vec.size(); i++) {
			std::cout << "Size of file: " << file_vec[i].size() << std::endl;
		}

		// Sort each vector individually, not multithreaded
		for(int i = 0; i < file_vec.size(); i++) {
			sort(file_vec[i].begin(), file_vec[i].end(), JagTableLineCompare(sort_by, schema, descending));
			std::ofstream buff_write;
			buff_write.open(box_vec[i], std::ios::out);

			for(abaxint j = 0; j < file_vec[i].size(); j++) {
				sprintf(lenbuf, FORMAT_STR, file_vec[i][j].line.length());
				buff_write.write(lenbuf, strlen(lenbuf));
				buff_write.write(file_vec[i][j].line.c_str(), file_vec[i][j].line.length());
			}

			std::cout << "Finished writing sorted file " << i << std::endl;
			buff_write.close();
			read_buff.close();
		}

		std::cout << "Finished writing temporary files." << std::endl;

		// Close any files left open
		write_file.close();
		// Merge the files together to form output file
		AbaxDataString sorted_file = mergeAllFiles(box_vec, sort_by, schema, box_vec.size(), descending);
		rename(sorted_file.c_str(), output.c_str());
		std::cout << "External Sort finished" << std::endl;
	}

	/**
	 * Merges and sort list of files together. Merges k-way. k must be greater than 1
	 */
	AbaxDataString mergeAllFiles(const std::vector<AbaxDataString>& box_vec, 
								 const std::vector<AbaxDataString>& sort_by, const JagSchemaRecord& schema, 
								 int k, bool descending = false) 
	{
		std::vector<AbaxDataString> merge_vec;
		std::queue<AbaxDataString> file_q;

		if(k < 2) {
			k = 2;
		}

		// Store temp files in queue
		for(int i = 0; i < box_vec.size(); i++) {
			merge_vec.push_back(box_vec[i]);
		}

		while(merge_vec.size() != 1) {
			int extra = merge_vec.size() % k;
			int num_runs = merge_vec.size()/k;
			std::vector<AbaxDataString> new_vec;

			if(num_runs == 0) {
				num_runs = 1;
				extra = 0;
			}
			
			std::vector<std::vector<AbaxDataString>> group_vec;

			for(int i = 0; i < num_runs; i++) {
				std::vector<AbaxDataString> merge_group;
				group_vec.push_back(merge_group);
			}
	
			for(int i = 0; i < merge_vec.size(); i++) {
				group_vec[i % num_runs].push_back(merge_vec[i]);
			}

			#pragma omp parallel for
			for(int i = 0; i < num_runs; i++) {
				AbaxDataString merged = mergeFiles(group_vec[i], sort_by, schema, num_runs, descending);
				std::cout << "Merge vector size: " << group_vec[i].size() << std::endl;
				new_vec.push_back(merged);

				#pragma omp parallel for
				for(int j = 0; j < group_vec[i].size(); j++) {
					remove((group_vec[i])[j].c_str());
				}
			}
			merge_vec = new_vec;
		}
		return merge_vec.front();
	}

	/**
	 * Merges the files in box_vec
	 */
	AbaxDataString mergeFiles( const std::vector<AbaxDataString>& box_vec, 
							   const std::vector<AbaxDataString>& sort_by, const JagSchemaRecord& schema, 
							   int k, bool descending = false) 
	{
		// Hash name for merged file
		uabaxint merge_hash = 0;

		#pragma omp parallel for
		for(int i = 0; i < box_vec.size(); i++) {
			merge_hash += hash(box_vec[i].c_str());
		}

		AbaxDataString merged = to_string(merge_hash) + ".dat";
		std::ofstream merge_file;
		merge_file.open(merged, std::ios::out);

		// Priority queue to compare ray record lines
		typedef std::priority_queue<JagTableLine, std::vector<JagTableLine>, JagTableLineCompare> pq_type;
		pq_type pq(JagTableLineCompare(sort_by, schema, !descending));

		// Unordered map to store information on each file and its buffer
		// <Key: file name, tuple<buffer, file its reading from, number of lines left in buffer, pointer inside buffer>>
		std::unordered_map<AbaxDataString, std::tuple<char*, std::ifstream*, int, int>> files;

		unsigned int numCPU = sysconf(_SC_NPROCESSORS_ONLN);
		if(numCPU < k) { k = numCPU; }
		int buf_size = (get_max_buf()/(box_vec.size()+1))/k;
		std::cout << "Buffer size: " << buf_size << std::endl;
		char* main_buf = (char*) malloc(MAIN_BUFF_LEN);		// Main buffer for sorting
		memset(main_buf, '\0', MAIN_BUFF_LEN);
		char lenbuf[BUFF_LEN + 1];
		memset(lenbuf, '\0', BUFF_LEN + 1);
		int rr_length;
		AbaxDataString full_line;

		//#pragma omp parallel for
		// Malloc a buffer for each file
		for(int i = 0; i < box_vec.size(); i++) {
			char* buf = (char*) malloc(buf_size);
			std::ifstream *file_ptr = new std::ifstream(box_vec[i], std::ios::in);
			int buf_ptr = 0;
			int num_lines = 0;
			int buf_left = buf_size;

			// Fill in the buffers
			while(!is_empty(*file_ptr)) {
				int pos = (*file_ptr).tellg();
				(*file_ptr).read(lenbuf, BUFF_LEN);
				rr_length = atol(lenbuf);
				char line[rr_length + 1];
				(*file_ptr).read(line, rr_length);
				full_line = AbaxDataString(lenbuf) + AbaxDataString(line);

				if(full_line.length() <= buf_left) {
					memcpy(buf+buf_size-buf_left, full_line.c_str(), full_line.length());
					num_lines++;
					buf_left -= full_line.length();
				}
				else {
					(*file_ptr).seekg(pos);
					break;
				}
			}
			files[box_vec[i]] = std::tuple<char*, std::ifstream*, int, int>(buf, file_ptr, num_lines, buf_ptr);
		}

		// Starts the pq with one line from each file	
		//#pragma omp parallel for
		for(int i = 0; i < box_vec.size(); i++) {
			/**
			char* buf = std::get<0>(files[box_vec[i]]);
			std::ifstream* file_ptr = std::get<1>(files[box_vec[i]]);
			int num_lines = std::get<2>(files[box_vec[i]]);
			int buf_ptr = std::get<3>(files[box_vec[i]]);
			*/

			// If there is a line to take then put in pq
			if(std::get<2>(files[box_vec[i]]) > 0) {
				int rr_length = atol(AbaxDataString(std::get<0>(files[box_vec[i]]), BUFF_LEN).c_str());
				AbaxDataString line(std::get<0>(files[box_vec[i]]) + std::get<3>(files[box_vec[i]]) + BUFF_LEN, rr_length);
				JagTableLine jag_line(line, box_vec[i]);
				pq.push(jag_line);
				std::get<3>(files[box_vec[i]]) += rr_length + BUFF_LEN;
				std::get<2>(files[box_vec[i]])--;

				// If no lines left check if file has more to put in buffer
				if(std::get<2>(files[box_vec[i]]) == 0) {
					int buf_left = buf_size;

					// Refill buffer
					while(!is_empty(*std::get<1>(files[box_vec[i]]))) {
						int pos = (*std::get<1>(files[box_vec[i]])).tellg();
						(*std::get<1>(files[box_vec[i]])).read(lenbuf, BUFF_LEN);
						abaxint rr_length = atol(lenbuf);
						char line[rr_length + 1];
						(*std::get<1>(files[box_vec[i]])).read(line, rr_length);
						AbaxDataString full_line = AbaxDataString(lenbuf) + AbaxDataString(line);

						if(rr_length <= buf_left) {
							memcpy(std::get<0>(files[box_vec[i]])+buf_size-buf_left, full_line.c_str(), full_line.length());
							std::get<2>(files[box_vec[i]])++;
							buf_left -= full_line.length();
						}
						else {
							(*std::get<1>(files[box_vec[i]])).seekg(pos);
							break;
						}
					}
					std::get<3>(files[box_vec[i]]) = 0;
				}
			}
		}

		int main_buf_ptr = 0;
		int lines_in_buf = 0;
		int progress = 0;
		memset(main_buf, '\0', buf_size);

		// Loop until no lines left, pops a line and writes into new file, reinserts a
		// new line from the file that it was taken from
		while(!pq.empty()) {

			JagTableLine curr = pq.top();
			pq.pop();
			sprintf(lenbuf, FORMAT_STR, curr.line.length());
			AbaxDataString full_line = AbaxDataString(lenbuf) + curr.line;

			if(buf_size < main_buf_ptr + full_line.length() || pq.empty()) {
				main_buf_ptr = 0;
				merge_file.write(main_buf, strlen(main_buf));
				lines_in_buf = 0;
				memset(main_buf, '\0', MAIN_BUFF_LEN);
			}

			memcpy(main_buf + main_buf_ptr, full_line.c_str(), full_line.length());
			main_buf_ptr += full_line.length();
			lines_in_buf++;
			char* buf = std::get<0>(files[curr.file_name]);
			std::ifstream* file_ptr = std::get<1>(files[curr.file_name]);
			int num_lines = std::get<2>(files[curr.file_name]);
			int buf_ptr = std::get<3>(files[curr.file_name]);

			// If there is a line to put in pq then take it
			if(num_lines > 0) {
				int rr_length = atol(AbaxDataString(buf + buf_ptr, BUFF_LEN).c_str());
				AbaxDataString line(buf + buf_ptr + BUFF_LEN, rr_length);
				JagTableLine jag_line(line, curr.file_name);
				pq.push(jag_line);
				buf_ptr += rr_length + BUFF_LEN;
				num_lines--;

				// Check if buffer is empty
				if(num_lines == 0) {
					int buf_left = buf_size;
					// Put as many lines in buffer as can fit
					while(!is_empty(*file_ptr)) {
						int pos = (*file_ptr).tellg();
						(*file_ptr).read(lenbuf, BUFF_LEN);
						abaxint rr_length = atol(lenbuf);
						char line[rr_length + 1];
						(*file_ptr).read(line, rr_length);
						AbaxDataString full_line = AbaxDataString(lenbuf) + AbaxDataString(line);

						if(rr_length <= buf_left) {
							memcpy(buf+buf_size-buf_left, full_line.c_str(), full_line.length());
							num_lines++;
							buf_left -= full_line.length();
						}
						else {
							(*file_ptr).seekg(pos);
							break;
						}
					}
					buf_ptr = 0;
				}
				std::tuple<char*, std::ifstream*, int, int> tup(buf, file_ptr, num_lines, buf_ptr);
				files[curr.file_name] = std::tuple<char*, std::ifstream*, int, int>(buf, file_ptr, num_lines, buf_ptr);
			}
			progress++;
		}

		// Empty the main buffer
		merge_file.write(main_buf, strlen(main_buf));

		// Free malloced buffers
		for(int i = 0; i < box_vec.size(); i++) {
			free(std::get<1>(files[box_vec[i]]));
			free(std::get<0>(files[box_vec[i]]));
		}

		free(main_buf);
		return merged;
	}

	/**
	 * Checks if pFile is about to reach eof.
	 */
	bool is_empty(std::ifstream& pFile) 
	{
		return pFile.peek() == std::ifstream::traits_type::eof();
	}

	/**
	 * Converts int to string
	 */
	AbaxDataString to_string(uabaxint number) 
	{
		std::ostringstream os;

		os << number;

		return os.str();
	}

	/**
	 * Hash function for strings
	 */
	uabaxint hash(const char *str) 
	{
 		uabaxint hash = 5381;
 		int c;

 		while (c = *str++) {
			hash = ((hash << 5) + hash) + c;
		}

 		return hash;
	}

	// Uses JagCfg object to get set max buffer size to allocate in bytes. Default is 3 MB
	int get_max_buf() 
	{
		JagCfg cfg;
		AbaxDataString max_buff = cfg.getValue(AbaxDataString("EXSORTBUFFER_SIZE"));
		if(max_buff == "") { return 3000000; }
		return std::stoi(max_buff)*1000000;
	}

	// Returns the size of the file
	int file_size(const AbaxDataString& file_name) 
	{
		std::ifstream file(file_name, std::ios::binary | std::ios::ate);

		return file.tellg();
	}
#endif
};
