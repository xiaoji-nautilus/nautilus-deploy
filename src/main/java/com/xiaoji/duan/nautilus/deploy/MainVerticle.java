package com.xiaoji.duan.nautilus.deploy;

import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import io.parallec.core.ParallecResponseHandler;
import io.parallec.core.ParallelClient;
import io.parallec.core.ResponseOnSingleTask;
import io.parallec.core.bean.ssh.SshLoginType;
import io.parallec.core.task.ParallelTaskState;
import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class MainVerticle extends AbstractVerticle {

	private AmqpBridge local = null;
	private ParallelClient deploySSHClient = null;
	private WebClient client = null;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		vertx.exceptionHandler(exception -> {
			error("Vertx exception caught.");
			System.exit(-1);
		});

		client = WebClient.create(vertx);
		deploySSHClient = new ParallelClient();

		local = AmqpBridge.create(vertx);
		connectLocalServer();
	}

	private void connectLocalServer() {
		local.start(config().getString("local.server.host", "sa-amq"), config().getInteger("local.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectLocalServer();
					} else {
						info("Local stomp server connected.");
						subscribeTrigger(config().getString("amq.app.id", "nautilus-deploy"));
					}
				});
	}

	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = local.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}

	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + getShortContent(received.body().encode()) + "]");
		JsonObject data = received.body().getJsonObject("body");

		String next = data.getJsonObject("context", new JsonObject()).getString("next");

		String target = data.getJsonObject("context", new JsonObject()).getString("target", "default");
		String ownerhashid = data.getJsonObject("context", new JsonObject()).getString("owner", "default");
		JsonObject payload = data.getJsonObject("context", new JsonObject()).getJsonObject("payload", new JsonObject());

		Future<JsonObject> future = Future.future();
		
		future.setHandler(handler -> {
			if (handler.succeeded()) {
				JsonObject nexto = handler.result();
				
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject().put("deployed", nexto));

				MessageProducer<JsonObject> producer = local.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.size() + "]");
			} else {
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject()
								.put("deployed", new JsonObject()
										.put("error", handler.cause().getMessage())));

				MessageProducer<JsonObject> producer = local.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.size() + "]");
			}

		});
		
		if ("nautilus-nashorn".equals(target)) {
			this.startDeployNashorn(future, ownerhashid, payload);
		}
		
		if ("nautilus-flow".equals(target)) {
			this.startDeployFlow(future, ownerhashid, payload);
		}
	}

	public static String getShortContent(String origin) {
		return origin.length() > 512 ? origin.substring(0, 512) : origin;
	}

	private JsonObject replaceNashornJs(JsonObject origin) {
		JsonObject replaced = origin.copy();
		
		JsonObject cleanfile = origin.getJsonObject("clean-file", new JsonObject());
		
		if (!cleanfile.isEmpty()) {
			String cleanjs = fetchJavaScript(cleanfile);

			if (StringUtils.isNotEmpty(cleanjs)) {
				replaced.put("clean", cleanjs);
			} else {
				replaced.put("clean", "");
			}
		} else {
			replaced.put("clean", "");
		}
		
		JsonObject shouldcleanfile = origin.getJsonObject("shouldclean-file", new JsonObject());

		if (!shouldcleanfile.isEmpty()) {
			
			String shouldcleanjs = fetchJavaScript(shouldcleanfile);
			
			if (StringUtils.isNotEmpty(shouldcleanjs)) {
				replaced.put("shouldclean", shouldcleanjs);
			} else {
				replaced.put("shouldclean", "");
			}

		} else {
			replaced.put("shouldclean", "");
		}

		return replaced;
	}
	
	private String fetchJavaScript(JsonObject jsfile) {
		String uri = jsfile.getString("uri", "");
		return uri;
	}
	
	private void startDeployNashorn(Future<JsonObject> deployFuture, String ownerhashid, JsonObject data) {
		String nashornid = data.getString("id", "nashorn");
		String hostname = data.getString("hostname", "nashorn");
		String hexnashornid = Hex.encodeHexString(DigestUtils.getMd2Digest().digest(nashornid.getBytes()));
		
		JsonObject payload = new JsonObject()
				.put("owner", ownerhashid)
				.put("hostname", hostname)
				.put("hexnashornid", hexnashornid)
				.put("nashorn", replaceNashornJs(data));
		
		String cleanjs = payload.getJsonObject("nashorn").getString("clean", "");
		String shouldcleanjs = payload.getJsonObject("nashorn").getString("shouldclean", "");
		
		vertx.executeBlocking(future -> {
			// Call some blocking API that takes a significant amount of time to
			// return

			ParallelTaskState result = sshCommand(deploySSHClient, "deploy", "pwd");
			result = sshCommand(deploySSHClient, "deploy", "rm -rf /root/" + ownerhashid + "/" + hexnashornid);
			if (result == ParallelTaskState.COMPLETED_WITHOUT_ERROR) {
				result = sshCommand(deploySSHClient, "deploy", "mkdir -p /root/" + ownerhashid + "/" + hexnashornid);
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode().replaceAll("'", "'\\\\''")
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_nashorn_configjson -o /root/" + ownerhashid + "/" + hexnashornid
								+ "/nautilus_nashorn_config.json");
				if (!StringUtils.isEmpty(cleanjs)) {
					result = sshCommand(deploySSHClient, "deploy",
							"curl -H \"Content-Type:application/json\" -X GET " 
									+ cleanjs + " -o /root/" + ownerhashid + "/" + hexnashornid
									+ "/clean.js");
				}
				if (!StringUtils.isEmpty(shouldcleanjs)) {
					result = sshCommand(deploySSHClient, "deploy",
							"curl -H \"Content-Type:application/json\" -X GET " 
									+ shouldcleanjs + " -o /root/" + ownerhashid + "/" + hexnashornid
									+ "/shouldclean.js");
				}
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode().replaceAll("'", "'\\\\''")
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_nashorn_dockercompose -o /root/" + ownerhashid + "/" + hexnashornid
								+ "/nn_" + ownerhashid + "_" + hexnashornid + ".yml");
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode().replaceAll("'", "'\\\\''")
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_nashorn_deployshell -o /root/" + ownerhashid + "/" + hexnashornid
								+ "/deploy-" + ownerhashid + ".sh");
				result = sshCommand(deploySSHClient, "deploy",
						"chmod +x /root/" + ownerhashid + "/" + hexnashornid + "/deploy-" + ownerhashid + ".sh");
				if (result == ParallelTaskState.COMPLETED_WITHOUT_ERROR) {
					result = sshCommand(deploySSHClient, "deploy",
							"cd /root/" + ownerhashid + "/" + hexnashornid + " && sh deploy-" + ownerhashid + ".sh");
				}
			}
			deploySSHClient.releaseExternalResources();
			future.complete(result);
		}, res -> {
			if (res.succeeded()) {
				System.out.println("The result is: " + res.result());
				deployFuture.complete(new JsonObject()
						.put("code", 0)
						.put("result", ((ParallelTaskState) res.result()).name()));
			} else {
				deployFuture.complete(new JsonObject()
						.put("code", -1)
						.put("result", res.cause().getMessage()));
			}
		});
		
	}

	private void startDeployFlow(Future<JsonObject> deployFuture, String ownerhashid, JsonObject data) {
		String flowid = data.getString("trigger", "flow");
		String hostname = data.getString("hostname", "flow");
		String hexflowid = Hex.encodeHexString(DigestUtils.getMd2Digest().digest(flowid.getBytes()));
		JsonObject payload = new JsonObject()
				.put("owner", ownerhashid)
				.put("hostname", hostname)
				.put("hexflowid", hexflowid)
				.put("flowstring", data.encode())
				.put("flow", data);
		
		vertx.executeBlocking(future -> {
			// Call some blocking API that takes a significant amount of time to
			// return

			ParallelTaskState result = sshCommand(deploySSHClient, "deploy", "pwd");
			result = sshCommand(deploySSHClient, "deploy", "rm -rf /root/" + ownerhashid + "/" + hexflowid);
			if (result == ParallelTaskState.COMPLETED_WITHOUT_ERROR) {
				result = sshCommand(deploySSHClient, "deploy", "mkdir -p /root/" + ownerhashid + "/" + hexflowid);
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode()
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_flow_configjson -o /root/" + ownerhashid + "/" + hexflowid
								+ "/nautilus_flow_config.json");
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode()
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_flow_dockercompose -o /root/" + ownerhashid + "/" + hexflowid
								+ "/nf_" + ownerhashid + "_" + hexflowid + ".yml");
				result = sshCommand(deploySSHClient, "deploy",
						"curl -H \"Content-Type:application/json\" -X POST --data '" + payload.encode()
								+ "' https://pluto.guobaa.com/aaj/generate/nautilus_flow_deployshell -o /root/" + ownerhashid + "/" + hexflowid
								+ "/deploy-" + ownerhashid + ".sh");
				result = sshCommand(deploySSHClient, "deploy",
						"chmod +x /root/" + ownerhashid + "/" + hexflowid + "/deploy-" + ownerhashid + ".sh");
				if (result == ParallelTaskState.COMPLETED_WITHOUT_ERROR) {
					result = sshCommand(deploySSHClient, "deploy",
							"cd /root/" + ownerhashid + "/" + hexflowid + " && sh deploy-" + ownerhashid + ".sh");
				}
			}
			deploySSHClient.releaseExternalResources();
			future.complete(result);
		}, res -> {
			if (res.succeeded()) {
				System.out.println("The result is: " + res.result());
				deployFuture.complete(new JsonObject()
						.put("code", 0)
						.put("result", ((ParallelTaskState) res.result()).name()));
			} else {
				deployFuture.complete(new JsonObject()
						.put("code", -1)
						.put("result", res.cause().getMessage()));
			}
		});
	}

	private ParallelTaskState sshCommand(ParallelClient client, String servertype, String cmd) {
		System.out.println(servertype + " ssh : " + cmd);
		vertx.eventBus().send("sockjsclientout", cmd);
		return client.prepareSsh()
				.setTargetHostsFromString(config().getString(servertype + ".server.ssh.host", "127.0.0.1"))
				.setSshLoginType(SshLoginType.PASSWORD)
				.setSshPort(config().getInteger(servertype + ".server.ssh.port", 2222))
				.setSshUserName(config().getString(servertype + ".server.ssh.username", "root"))
				.setSshPassword(config().getString(servertype + ".server.ssh.password", "1234")).setSshCommandLine(cmd)
				.execute(new ParallecResponseHandler() {

					@Override
					public void onCompleted(ResponseOnSingleTask res, Map<String, Object> paramMap) {
						String result = res.getResponseContent();
						if (StringUtils.isNotEmpty(result)) {
							String[] results = null;

							if (result.contains("\r\n")) {
								results = result.split("\r\n");
							} else if (result.contains("\r")) {
								results = result.split("\r");
							} else {
								results = result.split("\n");
							}

							for (String line : results) {
								String[] lines = null;

								if (line.contains("\r\n")) {
									lines = line.split("\r\n");
								} else if (line.contains("\r")) {
									lines = line.split("\r");
								} else {
									lines = line.split("\n");
								}

								for (String subline : lines) {
									error(subline);
								}
							}
						}
					}

				}).getState();
	}

	private void info(String log) {
		if (config().getBoolean("log.info", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void debug(String log) {
		if (config().getBoolean("log.debug", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void error(String log) {
		if (config().getBoolean("log.error", Boolean.TRUE)) {
			System.out.println(log);
		}
	}

	public static void main(String[] args) {
		String nashornid = "tttttttttesds";
		System.out.println(Hex.encodeHexString(DigestUtils.getMd2Digest().digest(nashornid.getBytes())));
		System.out.println(Hex.encodeHexString(DigestUtils.getMd5Digest().digest(nashornid.getBytes())));
		System.out.println(Hex.encodeHexString(DigestUtils.getSha1Digest().digest(nashornid.getBytes())));
		System.out.println(Hex.encodeHexString(DigestUtils.getSha256Digest().digest(nashornid.getBytes())));
		System.out.println(Hex.encodeHexString(DigestUtils.getSha384Digest().digest(nashornid.getBytes())));
		System.out.println(Hex.encodeHexString(DigestUtils.getShaDigest().digest(nashornid.getBytes())));
		
		System.out.println("'dddd'".replaceAll("'", "'\\\\''"));
	}
}
