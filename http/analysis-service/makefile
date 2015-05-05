CFYAML = ../../telemetry/util/cf-yaml-helper.py

SOURCES_BUCKET = telemetry-analysis-code
VERSION = 15

FILES = $(shell find * -name "*.py") \
		$(shell find * -name "*.sh") \
		$(shell find * -name "*.css") \
		$(shell find * -name "*.png") \
		$(shell find * -name "*.html")

telemetry-analysis-service.tar.gz: $(FILES)
	tar -czf $@ $^

analysis-service-stack.json: analysis-service-stack.yaml
	$(CFYAML) $< > $@

put: telemetry-analysis-service.tar.gz analysis-service-stack.json
	aws s3 cp telemetry-analysis-service.tar.gz s3://$(SOURCES_BUCKET)/v$(VERSION)/telemetry-analysis-service.tar.gz
	aws s3 cp analysis-service-stack.json s3://$(SOURCES_BUCKET)/v$(VERSION)/analysis-service-stack.json

clean:
	rm -f telemetry-analysis-service.tar.gz analysis-service-stack.json

.PHONY: put
