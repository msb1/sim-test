/* Push Kafka-arrival notifications into Dash without a polling dcc.Interval. */
(function connectTelemetrySocket() {
  const protocol = window.location.protocol === "https:" ? "wss" : "ws";
  const socket = new WebSocket(`${protocol}://${window.location.host}/telemetry/ws`);

  socket.onmessage = (event) => {
    window.dash_clientside.set_props("stream-version", { data: JSON.parse(event.data) });
  };
  socket.onclose = () => window.setTimeout(connectTelemetrySocket, 1000);
})();
