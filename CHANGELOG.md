# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## v1.1.3 - 2020-09-30

- Extend read_timeout to hopefully avoid more timeouts

## v1.1.2 - 2020-09-23

- Fix typo in get_out_path function

## v1.1.1 - 2020-09-17

- Added some more print statements (#75)
- Fixed testing suite (#76)

## v1.1.0 - 2020-09-05

## Changed

- Fixed dependency tests (#47)
- Add print statements to supplement logging, so you can see it working in real time
- Fix logic of main script (#52)
- Add support for upper case headers (#53)
- Better handling of missing values for jsonl (#61)
- Fix command line tool (#45)
- Add flake8 linting Github Action (#58)
- Actually compresses data when `compress-data = true` (#35)

## v1.0.0 - 2020-07-14

### Added

- Initial release, repurposed repo to use Goodtables