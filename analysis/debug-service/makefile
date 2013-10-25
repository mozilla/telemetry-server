CFYAML = ../cf-yaml-helper.py

SOURCES_BUCKET = jonasfj-telemetry-code
VERSION = 1

FILES = $(shell find * -name "*.py") \
		$(shell find * -name "*.sh") \
		$(shell find * -name "*.html")

telemetry-analysis-debug-service.tar.gz: $(FILES)
	echo $(FILES)
	tar -czf $@ $^

debug-service-stack.json: debug-service-stack.yaml
	$(CFYAML) $< > $@

put: telemetry-analysis-debug-service.tar.gz debug-service-stack.json
	aws s3 cp telemetry-analysis-debug-service.tar.gz s3://$(SOURCES_BUCKET)/v$(VERSION)/telemetry-analysis-debug-service.tar.gz
	aws s3 cp debug-service-stack.json s3://$(SOURCES_BUCKET)/v$(VERSION)/debug-service-stack.json

clean:
	rm -f telemetry-analysis-debug-service.tar.gz debug-service-stack.json

.PHONY: put
