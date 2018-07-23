#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/spirit/include/qi.hpp>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <list>
#include <map>
#include <queue>
#include <stdexcept>
#include <string.h>

#include "core/seastar.hh"
#include "core/reactor.hh"
#include "core/future-util.hh"
#include "core/gate.hh"
#include "core/sleep.hh"
#include "core/fstream.hh"
#include "core/app-template.hh"
#include "core/thread.hh"
#include "core/file.hh"
#include "net/stack.hh"
#include "util/log.hh"

using namespace seastar;

static uint16_t chat_port;

struct named_stream {
    sstring user_name;
    connected_socket s;
    socket_address a;
    input_stream<char> in;
    output_stream<char> out;
    named_stream(sstring name, connected_socket cs, socket_address ca) :
            user_name(std::move(name)),
            s(std::move(cs)),
            a(std::move(ca)),
            in(s.input()),
            out(s.output()) {}
    named_stream(connected_socket cs, socket_address ca) :
            user_name("NULL"),
            s(std::move(cs)),
            a(std::move(ca)),
            in(s.input()),
            out(s.output()) {}
};

enum {
    HAS_FILE_UPLOAD,
    HAS_FILE_REQUEST,
    HAS_MSG,
    EMPTY
};

enum {
    NEIGHBOR,
    ITSELF
};

struct buf_info_s {
    unsigned int context; // describes buffer context, see enum above
    sstring file_method;
    sstring file_name;
    uint64_t file_size;
    std::string payload;

    buf_info_s() :
        context(EMPTY),
        file_method("NULL"),
        file_name("NULL"),
        file_size(0),
        payload("NULL") {}
};

struct writer {
    output_stream<char> out;
    writer(file f) : out(make_file_output_stream(std::move(f))) {}
};

struct reader {
    input_stream<char> in;
    reader(file f) : in(make_file_input_stream(std::move(f))) {}
    reader(file f, file_input_stream_options options) : in(make_file_input_stream(std::move(f), std::move(options))) {}
};

typedef std::map<sstring, lw_shared_ptr<named_stream>> client_map;

class chat_room {
public:
    chat_room() {}
    ~chat_room() {}

    future<> start();
    future<> stop();

    future<lw_shared_ptr<named_stream>> register_user(connected_socket s, socket_address a) noexcept;
    future<lw_shared_ptr<named_stream>> wait_for_talker(lw_shared_ptr<named_stream> strms) noexcept;
    future<> start_dialog(lw_shared_ptr<named_stream> strms) noexcept;
    future<> send_message_to(int whom, sstring sender, client_map clients_map, std::string message) const noexcept;

    future<buf_info_s> watch_and_check_buffer(std::string unparsed_msg) noexcept;
    future<sstring> write_to_file(buf_info_s buf_info_t) const;
    future<temporary_buffer<char>> read_file(const sstring& file_name) const;

    int get_number_of_digits(const uint64_t& i) const;
    bool is_symbs_graph(const std::string& check) const;
    bool if_file_size_correct(const uint64_t& size) const;
    bool is_digit(const std::string& check) const;

    client_map clients_map = {};
    ssize_t conn_counter = 0;
    bool chat_ready = false;
};

future<> chat_room::start() {
    listen_options lo;
    lo.reuse_address = true;
    static thread_local semaphore limit(1);

    return keep_doing([this, lo] () {
        return with_semaphore(limit, conn_counter, [this, lo] {
            return do_with(listen(make_ipv4_address({chat_port}), lo),
                    [this] (auto& listener) {
                return listener.accept().then(
                        [this] (connected_socket s, socket_address a) {
                    s.set_nodelay(false);
                    s.set_keepalive(true);
                    s.set_keepalive_parameters(net::tcp_keepalive_params {std::chrono::seconds(60),
                                                                          std::chrono::seconds(60), 5});
                    conn_counter++; // although this is not accurate, SSIZE_MAX is big enough for all our affairs
                    register_user(std::move(s), std::move(a)).then([this] (auto strms) {
                        wait_for_talker(strms).then([this] (auto strms) {return start_dialog(strms).then([this] {
                                limit.signal(1);
                            });
                        });
                    });
                });
            });
        });
    });
}

future<> chat_room::stop() {
    try {
        this->~chat_room();
    } catch(...) {
        return make_exception_future(std::current_exception());
    }
    return make_ready_future<>();
}

future<lw_shared_ptr<named_stream>> chat_room::register_user(connected_socket s, socket_address a) noexcept {
    return do_with(std::move(s), std::move(a), lw_shared_ptr<named_stream>(), [this] (auto& s, auto& a, auto& strms) {
        strms = make_lw_shared<named_stream>(std::move(s), std::move(a));

        return strms->out.write("Please input a new username: ").then([this, strms] {
            return strms->out.flush();
        }).then([this, strms] {
            return do_with(sstring(), [this, strms] (auto& sender) {
                return strms->in.read().then([this, strms, &sender] (auto buf) mutable {
                    if (buf) {
                        sender = sstring(buf.begin(), buf.end() - 1);
                        if (clients_map.count(sender) != 0) {
                            return strms->out.write("User with such name has been already waiting for you, "
                                                    "you will be anonymous\n").then([strms, &sender] {
                                sender = "anonimous";
                                return strms->out.flush();
                            });
                        }
                        if(sender == "") {
                            sender = "anonimous_" + std::to_string(rand() % 100);
                            return strms->out.write("OK, you will be " + sender + "\n").then([strms] {
                                strms->out.flush();
                            });
                        }
                    } else {
                        // for case when user connects,
                        // and then disconnects without entering name
                        sender = "someone";
                        return make_ready_future<>();
                    }
                    return make_ready_future<>();
                }).then([this, strms, &sender] (){
                    boost::algorithm::to_lower(sender);
                    strms->user_name = std::move(sender);
                    clients_map.insert({strms->user_name, strms});
                    return make_ready_future<lw_shared_ptr<named_stream>>(std::move(strms));
                });
            });
        });
    });
}

future<lw_shared_ptr<named_stream>> chat_room::wait_for_talker(lw_shared_ptr<named_stream> strms) noexcept {
    if(clients_map.size() == 1) {
        send_message_to(ITSELF, strms->user_name, clients_map, "Waiting for another user to conect..");
    } else if (clients_map.size() == 2 && !chat_ready) { // make sure that the message below will be sended to one recipient
        chat_ready = true;
        send_message_to(NEIGHBOR, strms->user_name, clients_map, " has connected, you can chat now..");
    }
    return make_ready_future<lw_shared_ptr<named_stream>>(std::move(strms));
}

future<> chat_room::start_dialog(lw_shared_ptr<named_stream> strms) noexcept {
    return do_with(std::string(), [this, strms] (std::string& for_buf_info) {
        return repeat([this, strms, &for_buf_info] {
            return strms->in.read().then([this, strms, &for_buf_info] (auto buf) {
                if (buf) {
                    if (*(buf.end() - 1) != 10 && *(buf.end() - 1) != 4 ) { // see https://unicode-table.com
                        for_buf_info += std::string(buf.begin(), buf.end());
                        return make_ready_future<stop_iteration>(stop_iteration::no);
                    }
                    else {
                        for_buf_info += std::string(buf.begin(), buf.end() - 1);  //get last curent transfer part without '\n'
                        return watch_and_check_buffer(std::move(for_buf_info)).then([this, strms] (buf_info_s buf_info_t) {
                            switch(buf_info_t.context) {
                                case HAS_FILE_REQUEST:
                                    read_file(buf_info_t.file_name).then([this, strms] (auto buf) {
                                        if(buf) {
                                            send_message_to(ITSELF, strms->user_name, clients_map, std::string(buf.get(), buf.size()));
                                        } else {
                                            send_message_to(ITSELF, strms->user_name, clients_map, "No such file");
                                        }
                                    }).then([] {
                                        return stop_iteration::no;
                                    });
                                    break;
                                case HAS_MSG:
                                    return send_message_to(NEIGHBOR, strms->user_name, clients_map, ": "+ buf_info_t.payload).then([] {
                                        return stop_iteration::no;
                                    });
                                case HAS_FILE_UPLOAD:
                                    write_to_file(std::move(buf_info_t)).then([this, strms] (auto result) {
                                        if (result == "wfs_lmb") {
                                            send_message_to(ITSELF, strms->user_name, clients_map, "Cannot upload file. "
                                                                                                   "Possible reasons: wrong file size, lack of message body");
                                        }
                                        else {
                                            send_message_to(NEIGHBOR, strms->user_name, clients_map, " has uploaded file named " + result);
                                        }
                                        return make_ready_future<>();
                                        });
                                    break;
                                default:
                                    std::cout << "Should never happen" << std::endl;
                            }
                            return make_ready_future<stop_iteration>(stop_iteration::no);
                        });
                    }
                } else
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
            });
        }).then([this, strms] {
            return strms->out.close().handle_exception([](auto ep) {
                std::cerr << "Error: " << ep << std::endl;
            }).finally([this, strms] {
                return send_message_to(NEIGHBOR, strms->user_name, clients_map, " has disconnected");
                }).then([this, strms] {
                    clients_map.erase(strms->user_name);
                    chat_ready = false;
                    return strms->in.close().then([] {
                        return make_ready_future<>();
                    });
                 });
        });
    });
}

future<> chat_room::send_message_to(int whom, sstring sender, client_map clients_map, std::string message) const noexcept {
    return do_with(std::move(message), [whom, sender, clients_map] (auto& message){
        for(auto& i: clients_map) {
            if(i.first == sender && whom == ITSELF) {
                return i.second->out.write(message + "\n").then([i] {
                i.second->out.flush();
                });
            }
            else if (i.first != sender && whom == NEIGHBOR) {
                auto fin_msg = sender + message;
                return i.second->out.write(fin_msg + "\n").then([i] {
                    i.second->out.flush();
                });
            }
        }
        return make_ready_future<>();
    });
}

future<buf_info_s> chat_room::watch_and_check_buffer(std::string unparsed_msg) noexcept {

    return do_with(std::move(unparsed_msg), buf_info_s(), [this] (auto& unparsed_msg, buf_info_s& buf_info_t)  {
        std::string k = {};
        std::vector<std::string> pasred_msg_chunks = {};
        uint64_t len = 0;
        std::vector<std::string> parsed_msg;
        // simple parser for simple chat, this could also be e.g. boost spirit.
        // any wrong input which started with PUT or GET
        // (In addition to the file size, which is checked separately in write_to_file method)
        // will be ignored and output as is
        // if PUT/GET request is wrong, we will restore basic message via pasred_msg_chunks vector
        // there could be also a bundle of pointers to the basic meassage parts, not "k-len" scheme

        k = unparsed_msg.substr(0, unparsed_msg.find(" "));
        if (k == "PUT") {
            buf_info_t.file_method = "PUT";
            len += buf_info_t.file_method.size() + 1;
            unparsed_msg.erase(0, len);
            len = 0;
            pasred_msg_chunks.push_back(k);

            k = unparsed_msg.substr(0, unparsed_msg.find(" "));
            if (is_symbs_graph(k)  && k.length() != 0) {
                buf_info_t.file_name = k;
                len += buf_info_t.file_name.size() + 1;
                unparsed_msg.erase(0, len);
                len = 0;
                pasred_msg_chunks.push_back(k);

                k = unparsed_msg.substr(0, unparsed_msg.find(" "));
                if (is_digit(k) && k.length() != 0) {
                    buf_info_t.file_size = std::stoull(k);
                    len += get_number_of_digits(buf_info_t.file_size) + 1;
                    unparsed_msg.erase(0, len);
                    buf_info_t.payload = unparsed_msg; // push last part ("raw" message) into vecor
                    len = 0;
                    pasred_msg_chunks.push_back(k);

                    buf_info_t.context = HAS_FILE_UPLOAD;
                    return make_ready_future<buf_info_s>(std::move(buf_info_t));
                } else
                    goto finish_parsing;
            } else
                goto finish_parsing;
        }
        else if (k == "GET") {
            buf_info_t.file_method = "GET";
            len += buf_info_t.file_method.size() + 1;
            unparsed_msg.erase(0, len);
            len = 0;
            pasred_msg_chunks.push_back(k);

            k = unparsed_msg;
            k.erase(k.size()); // delete last symbol for correct handling file_name
            if (is_symbs_graph(k)  && k.length() != 0) {
                buf_info_t.file_name = k;
                len += buf_info_t.file_name.size() + 1;
                unparsed_msg.erase(0, len);
                len = 0;
                pasred_msg_chunks.push_back(k);

                buf_info_t.context = HAS_FILE_REQUEST;
                return make_ready_future<buf_info_s>(std::move(buf_info_t));
            } else
                goto finish_parsing;
        }
        finish_parsing:
            // payload could be empty rather NULL in buf_info_t constructor,
            // but then it becomes more difficult to find bugs deal with this struct
            buf_info_t.payload.clear();

            for (auto& i : pasred_msg_chunks) {
                buf_info_t.payload += i + " ";
            }
            buf_info_t.payload += unparsed_msg;
            buf_info_t.context = HAS_MSG;
            buf_info_t.file_method = "NULL";
            buf_info_t.file_name = "NULL";
            buf_info_t.file_size = 0;
        return make_ready_future<buf_info_s>(std::move(buf_info_t));
    });
}

future<sstring> chat_room::write_to_file(buf_info_s buf_info_t) const {

    return (buf_info_t.payload.length() != buf_info_t.file_size) || (!if_file_size_correct(buf_info_t.file_size)) ?
        make_ready_future<sstring>("wfs_lmb") :
        async([buf_info_t] {
            file f = open_file_dma(buf_info_t.file_name, open_flags::rw |
                                                     open_flags::create | open_flags::truncate).get0();
            f.truncate(buf_info_t.file_size).get();
            auto w = make_lw_shared<writer>(std::move(f));
            w->out.write(&(buf_info_t.payload[0]), buf_info_t.file_size).get();
            w->out.flush().get();
            w->out.close().get();
            return sstring(buf_info_t.file_name);
        });
}

future<temporary_buffer<char>> chat_room::read_file(const sstring& file_name) const {
    return file_exists(file_name).then([file_name] (auto ex){
        if (ex) {
            return open_file_dma(file_name, open_flags::ro).then([file_name](file f) {
                return do_with(std::move(f), [file_name](file& f) {
                    return f.size().then([&f, file_name](uint64_t size) {
                        return f.dma_read_bulk<char>(0, size);
                    }).finally([&f, file_name]() {
                        f.close();
                        remove_file(file_name);
                    });
                });
            }).handle_exception([file_name](std::exception_ptr ep) -> future<temporary_buffer<char>> {
               try {
                   std::rethrow_exception(std::move(ep));
               } catch (...) {
                   std::throw_with_nested(std::runtime_error(sstring("Could not read ") + file_name));
               }
            });
        } else
            return make_ready_future<temporary_buffer<char>>();
    });
}

int chat_room::get_number_of_digits (const uint64_t& i) const {
    return i > 0 ? log10(i) + 1 : 1;
}

bool chat_room::is_symbs_graph(const std::string& check) const {
    return std::all_of(check.begin(), check.end(), ::isgraph);
}

bool chat_room::if_file_size_correct(const uint64_t& size) const {
    // this is small construction, so brackets are added
    // just in case "for future"(as in some places above)
    if (!(size%4096)) {
        return true;
    }
    return false;
}

bool chat_room::is_digit(const std::string& check) const {
     return std::all_of(check.begin(), check.end(), ::isdigit);
}

future<> server() {
    auto new_chat_room = new chat_room();
    return new_chat_room->start().finally([new_chat_room] {
        return new_chat_room->stop().finally([new_chat_room] {});
    });
}

future<> f() {
    return parallel_for_each(boost::irange<unsigned>(0, smp::count),
            [] (unsigned c) {
        return smp::submit_to(c, server);
    });
}

int main(int argc, char** argv) {
    namespace bpo = boost::program_options;
    app_template::config app_cfg;
    app_cfg.name = "chat_port";
    app_template app(std::move(app_cfg));
    auto opt_add = app.add_options();
    opt_add("port,p", bpo::value<uint16_t>()->default_value(1234), "chat port");

    try {
        app.run(argc, argv, [&] {
            auto& configuration = app.configuration();
             auto port = configuration["port"].as<uint16_t>();
             chat_port = port;
            return parallel_for_each(boost::irange<unsigned>(0, smp::count),
                    [] (unsigned c) {
                return smp::submit_to(c, server);
            });
        });
    } catch(...) {
        std::cerr << "Couldn't start application: "
                  << std::current_exception() << std::endl;
        return 1;
    }
    return 0;
}
