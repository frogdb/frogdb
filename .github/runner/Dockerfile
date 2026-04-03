FROM myoung34/github-runner@sha256:10f124b019c94eeda67819cccaf7217aa5735fd1b78adda0d51981d23a7189df

# JavaScript-based GitHub Actions require Node.js on the runner.
RUN apt-get update && \
    apt-get install -y --no-install-recommends nodejs npm && \
    rm -rf /var/lib/apt/lists/*
