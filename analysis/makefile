FILES = downloader.py manager.py telemetry_schema.py worker.py telemetry-analysis-worker.sh

telemetry-analysis-worker.tar.gz: $(FILES)
	tar -czf telemetry-analysis-worker.tar.gz $^

put-worker-bundle: telemetry-analysis-worker.tar.gz
	s3cmd put -f telemetry-analysis-worker.tar.gz s3://jonasfj-telemetry-analysis/bundles/ 

clean:
	rm telemetry-analysis-worker-bundle.tar.gz

.PHONY: put-worker-bundle
