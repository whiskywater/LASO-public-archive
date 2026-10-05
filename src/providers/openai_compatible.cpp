#include <boost/beast.hpp>
#include <charconv>
#include <laso/providers/provider.hpp>

namespace laso {
namespace {
namespace beast = boost::beast;
namespace http = beast::http;
using tcp = asio::ip::tcp;

std::pair<std::string, std::string> parse_loopback_endpoint(const std::string &endpoint) {
  constexpr std::string_view prefix = "http://";
  if (!endpoint.starts_with(prefix))
    throw Error(ErrorCode::Configuration,
                "Local OpenAI endpoint must use http:// and a loopback address");
  const auto authority = endpoint.substr(prefix.size());
  if (authority.empty() || authority.find('/') != std::string::npos ||
      authority.find('@') != std::string::npos)
    throw Error(ErrorCode::Configuration,
                "Local OpenAI endpoint must not include a path or credentials");

  std::string host;
  std::string port = "80";
  if (authority.starts_with("[::1]")) {
    host = "::1";
    if (authority.size() > 5) {
      if (authority[5] != ':')
        throw Error(ErrorCode::Configuration, "Invalid local OpenAI endpoint");
      port = authority.substr(6);
    }
  } else {
    const auto colon = authority.rfind(':');
    host = colon == std::string::npos ? authority : authority.substr(0, colon);
    if (colon != std::string::npos)
      port = authority.substr(colon + 1);
    if (host != "127.0.0.1")
      throw Error(ErrorCode::Configuration, "Local OpenAI endpoint must use a loopback address");
  }
  unsigned value = 0;
  const auto [end, error] = std::from_chars(port.data(), port.data() + port.size(), value);
  if (error != std::errc{} || end != port.data() + port.size() || value == 0 || value > 65535)
    throw Error(ErrorCode::Configuration, "Invalid local OpenAI endpoint port");
  return {std::move(host), std::move(port)};
}

Json response_payload(const Json &response) {
  if (!response.is_object() || !response.contains("choices") ||
      !response.at("choices").is_array() || response.at("choices").empty())
    throw Error(ErrorCode::Provider, "Local model response has no completion choices");
  const auto &choice = response.at("choices").front();
  if (!choice.contains("message") || !choice.at("message").is_object() ||
      !choice.at("message").contains("content") || !choice.at("message").at("content").is_string())
    throw Error(ErrorCode::Provider, "Local model response has no text content");
  const auto content = choice.at("message").at("content").get<std::string>();
  const auto parsed = Json::parse(content, nullptr, false);
  return parsed.is_discarded() ? Json{{"text", content}} : parsed;
}
} // namespace

LocalOpenAICompatibleProvider::LocalOpenAICompatibleProvider(const std::string &endpoint) {
  auto parsed = parse_loopback_endpoint(endpoint);
  host_ = std::move(parsed.first);
  port_ = std::move(parsed.second);
}

Task<ModelResponse> LocalOpenAICompatibleProvider::generate(const ModelRequest &request,
                                                            ExecutionContext &context) {
  context.check();
  try {
    const auto executor = co_await asio::this_coro::executor;
    tcp::resolver resolver(executor);
    beast::tcp_stream stream(executor);
    stream.expires_at(context.deadline);
    const auto endpoints = co_await resolver.async_resolve(host_, port_, asio::use_awaitable);
    co_await stream.async_connect(endpoints, asio::use_awaitable);

    Json body{{"model", request.model},
              {"messages", Json::array({{{"role", "system"}, {"content", request.prompt}},
                                        {{"role", "user"}, {"content", request.input.dump()}}})},
              {"stream", false}};
    if (request.options.is_object())
      for (const auto &[key, value] : request.options.items())
        if (key == "temperature" || key == "max_tokens")
          body[key] = value;
    http::request<http::string_body> http_request{http::verb::post, "/v1/chat/completions", 11};
    http_request.set(http::field::host, host_);
    http_request.set(http::field::content_type, "application/json");
    http_request.body() = body.dump();
    http_request.prepare_payload();
    co_await http::async_write(stream, http_request, asio::use_awaitable);

    beast::flat_buffer buffer;
    http::response<http::string_body> http_response;
    co_await http::async_read(stream, buffer, http_response, asio::use_awaitable);
    context.check();
    if (http_response.result() != http::status::ok)
      throw Error(ErrorCode::Provider, "Local model service returned an unsuccessful response");
    boost::system::error_code shutdown_error;
    // NOLINTNEXTLINE(bugprone-unused-return-value): error_code overload reports via shutdown_error.
    stream.socket().shutdown(tcp::socket::shutdown_both, shutdown_error);
    if (shutdown_error && shutdown_error != asio::error::not_connected)
      throw Error(ErrorCode::Provider, "Local model connection did not close cleanly");
    co_return ModelResponse{response_payload(Json::parse(http_response.body())), request.model,
                            "local-openai"};
  } catch (const Error &) {
    throw;
  } catch (const boost::system::system_error &) {
    if (std::chrono::steady_clock::now() >= context.deadline)
      throw Error(ErrorCode::Timeout, "Local model request exceeded its deadline");
    throw Error(ErrorCode::Provider, "Local model request failed");
  }
}

ProviderHealth LocalOpenAICompatibleProvider::health() const {
  return {true, "configured loopback endpoint"};
}

ProviderMetadata LocalOpenAICompatibleProvider::metadata() const {
  ProviderMetadata result;
  result.name = "local-openai";
  result.version = "1";
  result.network = true;
  result.continuation_mode = ContinuationMode::Stateless;
  result.capabilities = {"chat-completions", "structured-output"};
  return result;
}
} // namespace laso
