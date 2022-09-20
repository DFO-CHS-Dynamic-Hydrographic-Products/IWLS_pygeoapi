# IWLS_pygeoapi

pygeoapi plugins to access and process water level and surface currents from the CHS Integrated Water Level System (IWLS) public API

Currently include a pygeoapi data provider plugin to interface with the IWLS database (https://api-iwls.dfo-mpo.gc.ca/swagger-ui/index.html?configUrl=/v3/api-docs/swagger-config) and a S-100 process plugin to convert observed, predicted and quality control forecasts water levels data to S-104 DCF8 format and HADCP surface currents to S-111 DCF8 format.


## Coding Guidelines

### General Guidelines
* Ensure that formatting complies with PEP8 guidlines https://peps.python.org/pep-0008/. Sample excerpt from the PEP8 guidelines:
    ```
    # Aligned with opening delimiter.
    foo = long_function_name(var_one, var_two,
                             var_three, var_four)

    # Add 4 spaces (an extra level of indentation) to distinguish arguments from the rest.
    def long_function_name(
            var_one, var_two, var_three,
            var_four):
        print(var_one)

    # Hanging indents should add a level.
    foo = long_function_name(
        var_one, var_two,
        var_three, var_four)
    ```
* Work should be adequately documented. Functions/methods should have docstrings describing their purpose and types in addition to inline comments with implementation details.
* Use type hinting for parameter and return values. See the python typing library for more information. See an example below:
    ```
    def foo(test_str: str) -> int:
    '''
    Foo an example test function that demonstrates documentation style and type hinting.

    :test_str: This is a test str (str)
    :return: A default hardcoded value (int)
    '''
        return 0
    ```
* Line length should not exceed 79 characters
* Use assert statements where possible, these can be later disabled in production by specifying a `-O` parameter when running the script. The asserts should also have a comment specified with a detailed message of the error. For example:
    ```
    assert type(test_str) == str, f'test_str variable is of type {type(test_str)} but should be str'
    ```
* Ensure commit messages are meaningful and describe the changes to the code base.
* Function length should not exceed screen size (rule of thumb) i.e. generally they should be short and you should not have to scroll to get to the bottom. If that is the case, the function should be futher subdivided. See https://refactoring.guru/smells/long-method

Some resources on code smells:
* https://refactoring.guru/refactoring/smells

## Code Review Guidelines

### Pull Request Lifecycle

1. It is standard to create an associated issue and pull a branch from that issue. Checkout out a branch from updated main. Branch names should start with the issue number. For example issue 12 called 'bug_fixes' would be 12-bug-fixes.
2. Implementat the issue using the above coding guidelines.
3. Prior to submitting a PR, you are required to test your code:
    * Code should compile
    * Run the tests under the `/tests` directory for both S111 and S104 files using the example curl requst listed below. All tests must pass.
4. Merge your branch locally with Main to avoid Merge Conflicts
5. Ensure your branch addresses all the features from the issue, follows style formatting guides (comments etc.) and is efficient.
4. Submit your PR and list the changes in the comments area in addition to any other concerns/comments you have. Assign a reviewer.
6. Complete changes suggested by reviewer
7. Test code again (see point 3) and ensure that code compiles

### Review Guidelines

1. Test the code (see point 3 in the Pull Request Lifecycle above). Ensure that all tests pass prior to continuing with any of the below steps.
2. Code should meet the following requirements:
  * Functionality: Does the proposed implementation fulfil the functionality from the issue?
  * Style: Is the implementation completed in a way that is efficient and well written?
  * Format: Does it follow the coding guidelines (i.e. comments/formatting for PEP8 guidelines etc.).
  * Error handling: Is there sufficient error handling to gracefully handle bugs or any other potential issues?
3. Respond to comments and address issues in a meaningful way
4. Once satisfied with the changes, merge the PR and ensure that the associated branch is deleted
