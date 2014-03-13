CFYAML = ../telemetry/util/cf-yaml-helper.py
SOURCES_BUCKET = jonasfj-telemetry-code
VERSION = 1

analysis-worker-stack.json: analysis-worker-stack.yaml
	$(CFYAML) $< > $@

put: analysis-worker-stack.json
	aws s3 cp analysis-worker-stack.json s3://$(SOURCES_BUCKET)/v$(VERSION)/analysis-worker-stack.json

clean:
	rm -f analysis-worker-stack.json

.PHONY: put
