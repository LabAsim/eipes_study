# Μελέτη ΕΙΠΕς (EIPEs study)

<a href="https://i.ibb.co/qskr3ZR/eipes-header.png">
    <img src="https://i.ibb.co/qskr3ZR/eipes-header.png" alt="logo header" style="width:700px;">
    <br/>
</a>

This repo contains the scripts to automate the whole process of sending personalized emails to the study participants.

You can find the flyer of the study [here](https://github.com/LabAsim/eipes_study/blob/master/media/eipes.png?raw=true).

## Table of Contents

* [Requirements](#Requirements)
* [Bioethics](#Bioethics)
* [Contact](#Contact)

## Requirements

* We use [pre-commit](https://pre-commit.com/) with black and ruff

* The `credentials.json` is taken from [gmail API](https://developers.google.com/gmail/api/guides).
You need to authenticate just once. After that, the newly-created `token.json` will be used.

* You need to create a .py file named `saved_tokens.py` that
it should contain the username (`GMAIL_USERNAME`) & password (`GMAIL_PASS`) of the gmail account

## Bioethics

The study was approved by the board of ethics and the administration of Aiginiteion Hospital.
You can find the approval [here](https://diavgeia.gov.gr/doc/%CE%A1%CE%9B%CE%92246%CE%A88%CE%9D2-4%CE%A4%CE%92).


## Contact

If you have any inquiries, email us to study.eipes(at)gmail.com
