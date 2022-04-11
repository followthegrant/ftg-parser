FROM debian:testing

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -qq -y update && apt-get -qq -y install python3-pip libpq-dev python3-icu git curl
COPY . /opt/ftg
RUN pip3 install -q -e /opt/ftg

# Download the ftm-typepredict model
# RUN mkdir /models/ && \
#     curl -o "/models/model_type_prediction.ftz" "https://public.data.occrp.org/develop/models/types/type-08012020-7a69d1b.ftz"

RUN mkdir /models/
COPY ./models/lid.176.ftz /models/lid.176.ftz

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

ENV DATA_ROOT=/data
ENV INGESTORS_LID_MODEL_PATH=/models/lid.176.ftz
ENV LOG_LEVEL=info

WORKDIR /opt/ftg
CMD ftg worker
