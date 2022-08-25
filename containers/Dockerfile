FROM gcr.io/deeplearning-platform-release/tf-gpu.1-15
LABEL email=sam.silverberg@gmail.com
LABEL author="Sam Silverberg"

RUN pip install \
    tensorboard \
    google-cloud-tpu \
    tokenizers \
    nltk \
    sentencepiece \
    ipykernel \
    transformers
WORKDIR /home/jupyter
RUN git clone https://github.com/google-research/bert

ENTRYPOINT [ "/entrypoint.sh" ]
CMD [ "/run_jupyter.sh" ]

