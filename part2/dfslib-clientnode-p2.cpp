#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = dfs_service::FileRequest;
using FileListResponseType = dfs_service::FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << "Sending Request of write access on file: " << filename << std::endl;

    // Initialize grpc objects and requests
    grpc::ClientContext context;
    dfs_service::WriteLockRequest request;
    dfs_service::WriteLockResponse response;
    request.set_filename(filename);
    request.set_client_id(client_id);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    // Send out gRPC request
    Status status = service_stub->RequestWriteLock(&context, request, &response);

    // Check response
    if (!status.ok()) {
        std::cout << "Failed to acquire write lock, error status code: " << status.error_code() << std::endl;
        std::cout << "Error message: " << status.error_message() << std::endl;
        return status.error_code();
    }
    std::cout << "Successfully acquired write lock on file name: " << filename << std::endl;
    return StatusCode::OK;

}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << "Sending Request of storing file: " << filename << std::endl;

    // Try to open file
    const std::string filepath = WrapPath(filename);
    std::ifstream file(filepath, std::ifstream::in | std::ifstream::binary);
    if (!file) {
        std::cerr << "Local file does not exist." << std::endl;
        return StatusCode::NOT_FOUND;
    }

    // Initiate ClientWriter
    dfs_service::StoreResponse response;
    grpc::ClientContext context;

    std::unique_ptr<ClientWriter<dfs_service::StoreChunk> > writer = service_stub->
            StoreFile(&context, &response);

    // Initiate file buffer and request message
    const size_t BufferSize = CHUNK_SIZE; // 64 KB chunks
    char buffer[BufferSize];

    dfs_service::StoreChunk chunk;
    chunk.set_filename(filename);
    chunk.set_crc(dfs_file_checksum(filepath, &crc_table));

    struct stat file_stat;
    lstat(filepath.c_str(), &file_stat);
    chunk.set_mtime(file_stat.st_mtime);

    // Try to acquire write lock of target file
    StatusCode writeLockStatus = RequestWriteAccess(filename);
    if (writeLockStatus != StatusCode::OK) {
        return writeLockStatus;
    }

    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // Repeatedly read the file and copy into stream message
    while (!file.eof()) {
        file.read(buffer, BufferSize);
        size_t bytesRead = file.gcount();
        if (bytesRead == 0) {
            break;
        }

        // Copy read file into chunk message
        chunk.set_data(buffer, bytesRead);

        // Send out current chunk
        if (!writer->Write(chunk)) {
            std::cerr << "Write error." << std::endl;
            break;
        }
    }

    // Finish the stream
    writer->WritesDone();
    Status status = writer->Finish();

    // Check response
    if (!status.ok()) {
        std::cout << "Failed to store file with error status code: " << status.error_code() << std::endl;
        std::cout << "Error message: " << status.error_message() << std::endl;
        return status.error_code();
    }
    std::cout << "Successfully stored file." << std::endl;
    return StatusCode::OK;
}


grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    std::cout << "-----------------------------------------------------------" << std::endl;
    std::cout << "Sending Request of fetching file: " << filename << std::endl;

    // Initialize grpc objects and requests
    grpc::ClientContext context;
    dfs_service::FetchRequest request;
    request.set_filename(filename);

    // Gather file info for validation
    const std::string filepath = WrapPath(filename);
    struct stat file_stat;
    if (lstat(filepath.c_str(), &file_stat) == 0) {
        // File exists
        request.set_crc(dfs_file_checksum(filepath, &crc_table));
        request.set_mtime(file_stat.st_mtime);
    }

    // Start to fetch file
    std::cout << "Storing file at: " << filepath << std::endl;
    // Store at temp path
    const std::string temp_filepath = filepath + ".tmp";
    std::fstream file(temp_filepath, std::ios::out | std::ios::trunc | std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Failed to initiate local fd." << std::endl;
        return StatusCode::CANCELLED;
    }

    dfs_service::FetchChunk chunk;
    std::unique_ptr<ClientReader<dfs_service::FetchChunk> > reader = service_stub->FetchFile(&context, request);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    int64_t server_mtime = 0;
    // Try and start to receive file
    while (reader->Read(&chunk)) {
        if (!file.write(chunk.data().data(), chunk.data().size())) {
            std::cerr << "Failed to write file." << std::endl;
            return StatusCode::CANCELLED;
        }
        // Record server mtime
        if (server_mtime == 0) {
            server_mtime = chunk.mtime();
        }
    }
    Status status = reader->Finish();
    file.close();

    // Cleanup if error occurred
    if (!status.ok()) {
        std::remove(temp_filepath.c_str());
        std::cout << "Failed to fetch file with error status code: " << status.error_code() << std::endl;
        std::cout << "Error message: " << status.error_message() << std::endl;
        return status.error_code();
    }
    std::remove(filepath.c_str());           // Delete old (if exists)
    std::rename(temp_filepath.c_str(), filepath.c_str());  // Rename temp
    // Set mtime to match with server
    struct utimbuf new_times;
    new_times.actime = server_mtime;
    new_times.modtime = server_mtime;
    utime(filepath.c_str(), &new_times);

    std::cout << "Successfully fetched file." << std::endl;
    return StatusCode::OK;
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
}

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //


    callback();

}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //



            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//

