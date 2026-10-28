window.onload = function() {
  //<editor-fold desc="Changeable Configuration Block">

  // the following lines will be replaced by docker/configurator, when it runs in a docker-container
  window.ui = SwaggerUIBundle({
    spec: location.host,
    urls:  [{"url":window.location.protocol+"//"+location.host+"/swagger/serve.swagger.json","name":"RayServe Service"},
            {"url":window.location.protocol+"//"+location.host+"/swagger/error.swagger.json","name":"Errors API"},
            {"url":window.location.protocol+"//"+location.host+"/swagger/job.swagger.json","name":"RayJob Service"},
            {"url":window.location.protocol+"//"+location.host+"/swagger/config.swagger.json","name":"ComputeTemplate Service"},
            {"url":window.location.protocol+"//"+location.host+"/swagger/cluster.swagger.json","name":"Cluster Service"}],
    dom_id: '#swagger-ui',
    deepLinking: true,
    presets: [
      SwaggerUIBundle.presets.apis,
      SwaggerUIStandalonePreset
    ],
    plugins: [
      SwaggerUIBundle.plugins.DownloadUrl
    ],
    layout: "StandaloneLayout"
  });

  //</editor-fold>
};
