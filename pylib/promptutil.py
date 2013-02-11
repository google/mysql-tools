"""Utility functions to facilitate user interactions.

Supplies several methods to prompt the user to answer a question.
Takes care of input validation, default responses, etc.
"""

__author__ = "lxnay@google.com (Fabio Erculiani)"

import readline  # side effect is that we get a better raw_input implementation


def PromptYN(message):
  """Prompts a user with a Yes/No question;  Returns a boolean.

  Args:
    message: the message prompted to the user.
  Returns:
    True if answer was Yes, False if answer was No.
  """
  input_message = "%s [Yes/No]" % (message,)
  while True:
    try:
      answer = raw_input(input_message).lstrip().lower()
      if answer.startswith("y"):
        return True
      elif answer.startswith("n"):
        return False
      elif not answer:
        continue
      print "Invalid response. Try again."
    except EOFError:
      # EOFError exceptions are caught by default and ignored
      continue
