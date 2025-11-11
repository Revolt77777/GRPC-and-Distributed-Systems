#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <dirent.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-shared-p1.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;


//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    Status StoreFile(::grpc::ServerContext* context, ::grpc::ServerReader< ::dfs_service::StoreChunk>* reader, ::dfs_service::StoreResponse* response) override {
        dfs_service::StoreChunk chunk;
        std::cout << "-----------------------------------------------------------" << std::endl;
        std::cout << "Receiving file..." << std::endl;
        // Read first chunk to get filename
        if (!reader->Read(&chunk)) {
            std::cerr << "Failed to read first file chunk" << std::endl;
            return Status(StatusCode::CANCELLED, "No data in file");
        }
        const std::string filename = chunk.filename();
        const std::string filepath = WrapPath(filename);
        std::cout << "Storing file at: " << filepath << std::endl;

        // Open or create the file and write the first chunk
        std::fstream file(filepath, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!file.is_open()) {
            std::cerr << "Failed to open file" << std::endl;
            return Status(StatusCode::CANCELLED, "Can't open file");
        }
        if (!file.write(chunk.data().data(), chunk.data().size())) {
            std::cerr << "Failed to write file" << std::endl;
            return Status(StatusCode::CANCELLED, "Can't write file");
        }

        // Repeatedly receive and write chunks if necessary
        while (reader->Read(&chunk)) {
            if (!file.write(chunk.data().data(), chunk.data().size())) {
                std::cerr << "Failed to write file" << std::endl;
                return Status(StatusCode::CANCELLED, "Can't write file");
            }
        }

        response->set_filename(filename);
        struct stat filestat;
        if (stat(filepath.c_str(), &filestat) == 0) {
            response->set_mtime(filestat.st_mtime);
        }
        std::cout << "Successfully stored file at: " << filepath << std::endl;
        return Status::OK;
    }

    Status FetchFile(::grpc::ServerContext* context, const ::dfs_service::FetchRequest* request, ::grpc::ServerWriter< ::dfs_service::FetchChunk>* writer) override {
        std::cout << "-----------------------------------------------------------" << std::endl;
        std::cout << "Receiving request to fetch file: " << request->filename() << std::endl;

        // Try to open file
        const std::string filename = request->filename();
        const std::string filepath = WrapPath(filename);
        std::ifstream file(filepath, std::ifstream::in | std::ifstream::binary);
        if (!file) {
            std::cerr << "File does not exist." << std::endl;
            return Status(StatusCode::NOT_FOUND, "File does not exist.");
        }

        // Initiate file buffer and chunk message
        const size_t BufferSize = dfs_shared::CHUNK_SIZE; // 64 KB chunks
        char buffer[BufferSize];

        dfs_service::FetchChunk chunk;

        // Repeatedly read the file and copy into stream message
        while (!file.eof()) {
            file.read(buffer, BufferSize);
            size_t bytesRead = file.gcount();
            if (bytesRead == 0) {
                std::cerr << "File read error." << std::endl;
                return Status(StatusCode::CANCELLED, "File read error.");
            }

            // Copy read file into chunk message
            chunk.set_data(buffer, bytesRead);

            // Send out current chunk
            if (!writer->Write(chunk)) {
                std::cerr << "Write error." << std::endl;
                return Status(StatusCode::CANCELLED, "Write error.");
            }
        }

        /*
        chunk.set_filename(filename);
        struct stat filestat;
        if (stat(filepath.c_str(), &filestat) == 0) {
            chunk.set_mtime(filestat.st_mtime);
        }
        writer->Write(chunk);*/
        std::cout << "Successfully fetched file." << std::endl;
        return Status::OK;
    };
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//
