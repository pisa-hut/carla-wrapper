FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive

RUN usermod -l carla ubuntu
RUN groupmod -n carla ubuntu

COPY --from=docker.io/tonychi/carla:0.9.16 --chown=carla:carla /opt/carla /opt/carla

RUN <<EOF
    apt update
    apt install -y \
        git ca-certificates \
        build-essential g++-12 cmake ninja-build libvulkan1 \
        python3 python3-dev python3-pip python3-venv autoconf \
        wget curl rsync unzip git git-lfs libpng-dev libtiff5-dev \
        libjpeg-dev libgl-dev libglib2.0-dev
    rm -rf /var/lib/apt/lists/*
EOF


ADD https://security.ubuntu.com/ubuntu/pool/main/t/tiff/libtiff5_4.3.0-6_amd64.deb /tmp/libtiff5-dev.deb
RUN dpkg -i /tmp/libtiff5-dev.deb && rm /tmp/libtiff5-dev.deb

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
# Two srunner sources ship as different image tags (see CI matrix):
#   - :main   → derekwuchengyu fork (default for back-compat)
#   - :native → upstream carla-simulator/scenario_runner
ARG SRUNNER_GIT=https://github.com/derekwuchengyu/scenario_runner.git
ADD ${SRUNNER_GIT} /opt/scenario_runner
# Fork ships custom example Catalogs under srunner/examples/Catalogs;
# upstream srunner doesn't. Matrix sets WITH_CATALOGS=0 for :native.
ARG WITH_CATALOGS=1
RUN if [ "$WITH_CATALOGS" = "1" ]; then \
    cp -r /opt/scenario_runner/srunner/examples/Catalogs /opt/Catalogs; \
    fi

USER carla
WORKDIR /app
COPY --chown=carla:carla ./pyproject.toml .
COPY --chown=carla:carla ./uv.lock .
RUN uv sync --locked --no-dev --no-install-project
ENV PYTHONPATH=/opt/scenario_runner/:/opt/carla/PythonAPI/carla/
COPY --chown=carla:carla  . .
RUN uv sync --locked --no-dev
RUN uv pip install /opt/carla/PythonAPI/carla/dist/carla-0.9.16-cp310-cp310-linux_x86_64.whl
RUN uv pip install -r /opt/scenario_runner/requirements.txt

ENV PORT=50051
ENV CARLA_PORT=2000
ENV CARLA_TM_PORT=8000
ENV CARLA_TIMEOUT=30
ENV PYTHONFAULTHANDLER=1

ENTRYPOINT [ "/bin/bash" ]
CMD [ "/app/entrypoint.sh" ]
