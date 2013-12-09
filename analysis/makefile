CFYAML = ./cf-yaml-helper.py
SOURCES_BUCKET = jonasfj-telemetry-code
VERSION = 1

FILES = \
	downloader.py 		\
	manager.py 				\
	worker.py					\
	utils.py

analysis-worker.tar.gz: $(FILES)
	tar -czf $@ $^

analysis-worker-stack.json: analysis-worker-stack.yaml
	$(CFYAML) $< > $@

put: analysis-worker.tar.gz analysis-worker-stack.json
	aws s3 cp analysis-worker.tar.gz s3://$(SOURCES_BUCKET)/v$(VERSION)/analysis-worker.tar.gz
	aws s3 cp analysis-worker-stack.json s3://$(SOURCES_BUCKET)/v$(VERSION)/analysis-worker-stack.json

clean:
	rm -f telemetry-analysis-worker-bundle.tar.gz analysis-worker-stack.json

.PHONY: put
