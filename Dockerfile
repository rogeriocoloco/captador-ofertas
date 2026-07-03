FROM node:22-alpine
WORKDIR /app
COPY captador.mjs .
ENV NODE_ENV=production
ENV PORT=3711
EXPOSE 3711
CMD ["node","captador.mjs"]
