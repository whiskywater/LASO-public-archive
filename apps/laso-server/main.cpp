#include <CLI/CLI.hpp>
#include <boost/system/system_error.hpp>
#include <csignal>
#include <iostream>
#include <laso/api/api.hpp>
#include <laso/artifacts/server.hpp>
#include <memory>
#include <sys/stat.h>
#include <system_error>

int main(int argc, char **argv) {
  CLI::App app{"LASO server"};
  std::string config_path, host;
  unsigned port = 0;
  app.add_option("--config", config_path);
  app.add_option("--host", host);
  app.add_option("--port", port);
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError &e) {
    return app.exit(e);
  }
  try {
    umask(0077);
    std::map<std::string, std::string> overrides;
    if (!host.empty())
      overrides["api_host"] = host;
    if (port)
      overrides["api_port"] = std::to_string(port);
    auto config = laso::load_config(config_path, overrides);
    laso::Executor executor(config.workers);
    laso::Service service(executor.context(), config);
    laso::LocalDevelopmentIdentity identity;
    laso::Api api(service, identity);
    laso::HttpServer server(executor.context(), api, config.api_host,
                            static_cast<unsigned short>(config.api_port));
    std::unique_ptr<laso::ArtifactHttpServer> artifact_server;
    if (config.artifact_service_port != 0) {
      artifact_server = std::make_unique<laso::ArtifactHttpServer>(
          executor.context(), service.artifacts(), config.artifact_service_host,
          static_cast<unsigned short>(config.artifact_service_port), config.artifact_service_token,
          config.max_artifact_bytes);
    }
    laso::asio::signal_set signals(executor.context(), SIGINT, SIGTERM);
    signals.async_wait([&](const boost::system::error_code &, int) {
      if (artifact_server)
        artifact_server->stop();
      server.stop();
      service.shutdown();
    });
    server.start();
    if (artifact_server)
      artifact_server->start();
    executor.start();
    executor.join();
    return 0;
  } catch (const laso::Error &e) {
    std::cerr << e.what() << '\n';
    return 1;
  } catch (const std::system_error &e) {
    // Report the portable code message without echoing the failing path.
    std::cerr << "LASO server initialization failed: " << e.code().message() << '\n';
    return 1;
  } catch (const boost::system::system_error &e) {
    // Report only the portable error-category message: diagnostic text must
    // not echo configuration, paths, environment values, or database details.
    std::cerr << "LASO server initialization failed: " << e.code().message() << '\n';
    return 1;
  } catch (...) {
    std::cerr << "LASO server initialization failed\n";
    return 1;
  }
}
