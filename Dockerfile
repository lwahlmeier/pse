FROM scratch
ARG VERSION
COPY bin/pse-${VERSION} /pse
EXPOSE 8681
CMD ["/pse"]
