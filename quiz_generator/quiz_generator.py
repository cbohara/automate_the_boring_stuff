import sys
import random
import states


def main():
    """Create 36 distinct quizzes with questions and answers in random order, along with its associated answer key."""
    for num in range(1, 36):
        # create quiz and answer key files
        quiz = open('quiz{}.txt'.format(num), 'w')
        answers = open('answers{}.txt'.format(num), 'w')

        # write out the header for the quiz
        quiz.write('Name:\nDate:\nPeriod:\n')
        quiz.write('State Capitals Quiz (Form #{})\n'.format(num))

        # import dictionary in states.py 
        state_capitals_dict = states.capitals

        # shuffle order of states
        states = list(state_capitals_dict.keys())
        random.shuffle(states)


if __name__ == "__main__":
    sys.exit(main())
