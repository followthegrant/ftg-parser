FROM ghcr.io/investigativedata/ftm-docker:main

LABEL org.opencontainers.image.title "FollowTheGrant ETL parser worker"
LABEL org.opencontainers.image.source https://github.com/followthegrant/ftg-parser

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq -y update && apt-get -qq -y install libpq-dev

RUN pip install -U pip setuptools

# Download the ftm-typepredict model
# RUN mkdir /models/ && \
#     curl -o "/models/model_type_prediction.ftz" "https://public.data.occrp.org/develop/models/types/type-08012020-7a69d1b.ftz"
COPY ./models /models

RUN pip install spacy

RUN python3 -m spacy download en_core_web_sm \
 && python3 -m spacy download de_core_news_sm \
 && python3 -m spacy download fr_core_news_sm \
 && python3 -m spacy download es_core_news_sm \
 && python3 -m spacy download ru_core_news_sm \
 && python3 -m spacy download pt_core_news_sm \
 && python3 -m spacy download ro_core_news_sm \
 && python3 -m spacy download mk_core_news_sm \
 && python3 -m spacy download el_core_news_sm \
 && python3 -m spacy download pl_core_news_sm \
 && python3 -m spacy download it_core_news_sm \
 && python3 -m spacy download lt_core_news_sm \
 && python3 -m spacy download nl_core_news_sm \
 && python3 -m spacy download nb_core_news_sm \
 && python3 -m spacy download da_core_news_sm

RUN mkdir -p /app/followthegrant/followthegrant
COPY ./followthegrant /app/followthegrant/followthegrant
COPY ./setup.py /app/followthegrant
COPY ./setup.cfg /app/followthegrant
COPY ./README.md /app/followthegrant
COPY ./VERSION /app/followthegrant
RUN pip install -e /app/followthegrant

ENV DATA_ROOT=/data
ENV INGESTORS_LID_MODEL_PATH=/models/lid.176.ftz
ENV LOG_LEVEL=info

WORKDIR /app/followthegrant
CMD ftg worker
