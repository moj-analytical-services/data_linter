# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## 5.0.0 - 2021-03-15

- Created Pandas Validator (now replaces frictionless as the default validator) ([issue #120](https://github.com/moj-analytical-services/data_linter/issues/120), [issue #98](https://github.com/moj-analytical-services/data_linter/issues/98))
- Enabled parallelisation of validators ([#122](https://github.com/moj-analytical-services/data_linter/issues/122))
- Migrated to the new metadata schemas ([#140](https://github.com/moj-analytical-services/data_linter/issues/140))
- Single process validator can run locally ([#121](https://github.com/moj-analytical-services/data_linter/issues/121))
- Full log now writes to it's own folder ([#130](https://github.com/moj-analytical-services/data_linter/issues/130))

```
[ALL CLOSED ISSUES]
- issue #140 
- issue #139
- issue #133 
- issue #132
- issue #131 
- issue #130 
- issue #129 
- issue #128 
- issue #125
- issue #122 
- issue #121 
- issue #120 
- issue #110 
- issue #100 
- issue #98
- issue #87
- issue #70
```

## 4.1.0 - 2020-11-24

- Add pandas-kwargs to table params to pass through to Great Expectations (#112)
- Update dependencies
## 4.0.0 - 2020-11-03

- Split out the codebase to make it easier to add new validators. These have to conform to the validator base class `data_linter/validators/base.py`. (#101)
- Added a great expectations validator. (#103)
- Suprise! Reverted back to frictionless (from the previous revert in v3 to goodtables) (#102)
- Improved logging now log up to level INFO is written to standard out and level DEBUG to S3 log.
- Added ability for user to define how data is written to S3. Specifically if you want it to be partitioned by timestamp or not.
- Dropping the `v` from our releases.

## v3.0.0 - 2020-10-23

- Revert back to goodtables

## v2.0.1 - 2020-10-21

- Fix log_path being called before assignment

## v2.0.0 - 2020-10-19

- Upgrade from `goodtables` to `frictionless` package (#57)
- (Hopefully) address aws read timeout issue (#79)
- Review printing and logging (#86)
- Separating out functionality so that users can provide a config stored in memory rather than a file (#84)
- Add option for defining the timestamp partition name (#85)

## v1.1.4 - 2020-10-05

- Minor logic change when all_must_pass is set to True
- improved logging

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
