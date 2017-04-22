import sys
import random
import states


def generate_answers(states, capitals):
    """Generate questions for quiz."""
    for num in range(50):
        # get right answer
        correct = capitals[states[num]]
        # generate all possible answers
        incorrect = list(capitals.values())
        # remove correct answer from incorrect list
        del incorrect[incorrect.index(correct)]
        # take 3 random values from all possible incorrect values in the list
        incorrect = random.sample(incorrect, 3)
        # generate list of 4 answer options - 1 correct, 3 incorrect
        answer_options = incorrect + list(correct)
        # shuffle answer options
        return random.shuffle(answer_options)


def main():
    """Create 36 distinct quizzes with questions and answers in random order, along with its associated answer key."""
    try:
        total_quizzes = sys.argv[1]
    except IndexError:
        print('python3 quiz_generator.py [number of quizzes and associated answer keys you want generated]')
    else:
        for num in range(1, total_quizzes+1):
            # create quiz and answer key files
            quiz = open('quiz{}.txt'.format(num), 'w')
            answers = open('answers{}.txt'.format(num), 'w')

            # write out the header for the quiz
            quiz.write('Name:\nDate:\nPeriod:\n')
            quiz.write('State Capitals Quiz (Form #{})\n'.format(num))

            # import dictionary of {'state': 'capital'} from states.py 
            capitals = states.capitals

            # shuffle order of states
            states = list(capitals.keys())
            random.shuffle(states)

            # loop through all 50 states, making a question for each
            generate_answers(states, capitals)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
